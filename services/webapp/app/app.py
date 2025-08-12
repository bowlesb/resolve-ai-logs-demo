"""
Minimal Dash web UI for the log distributor demo.

- Persists analyzer ON/OFF state and weights in MongoDB.
- Shows recent log counts from Graylog (per analyzer).
- Polls distributor /health and renders per-analyzer circuit breaker status.
"""

from __future__ import annotations

import logging

from dash import Dash, html, dcc, Input, Output, State
import plotly.graph_objects as go
from pymongo.errors import PyMongoError

from app.helpers import (
    LOG,
    ANALYZERS,
    WINDOW_SECS,
    REFRESH_MS,
    ANALYZERS_COL,
    WEIGHTS_COL,
    get_states_and_weights,
    graylog_count,
    fetch_breakers,
    render_breaker_table,
    analyzer_state_control,
    DISTRIBUTOR_API,
)

APP = Dash(__name__)
APP.title = "Log Distribution Dashboard"
LOGGER = logging.getLogger("webapp.app")


def serve_layout():
    """Build and return the app layout with current analyzer states and weights."""
    states_by_analyzer, weights_by_analyzer = get_states_and_weights()

    # Controls (right panel)
    analyzer_controls = []
    for analyzer_name in ANALYZERS:
        analyzer_controls.append(
            html.Div(
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "justifyContent": "space-between",
                    "margin": "6px 0",
                },
                children=[
                    html.Div(analyzer_name, style={"fontWeight": "600"}),
                    dcc.RadioItems(
                        id=f"state-{analyzer_name}",
                        options=[
                            {"label": "ON", "value": "on"},
                            {"label": "OFF", "value": "off"},
                        ],
                        value=(
                            "on"
                            if states_by_analyzer.get(analyzer_name, True)
                            else "off"
                        ),
                        inline=True,
                        labelStyle={"marginRight": "12px"},
                    ),
                ],
            )
        )

    weights_controls = [
        html.Div(
            style={
                "display": "flex",
                "alignItems": "center",
                "justifyContent": "space-between",
                "margin": "6px 0",
            },
            children=[
                html.Label(analyzer_name, style={"marginRight": "10px"}),
                dcc.Input(
                    id=f"w-{analyzer_name}",
                    type="number",
                    min=0,
                    step=0.01,
                    value=weights_by_analyzer.get(analyzer_name, 0.25),
                ),
            ],
        )
        for analyzer_name in ANALYZERS
    ]

    return html.Div(
        style={
            "backgroundColor": "#111",
            "color": "#eee",
            "minHeight": "100vh",
            "padding": "20px",
        },
        children=[
            html.H2(
                f"Data Ingestion Distribution (last {WINDOW_SECS} sec)",
                style={"textAlign": "center"},
            ),
            html.Div(
                style={"display": "flex", "gap": "30px"},
                children=[
                    html.Div(
                        style={"flex": "1", "minWidth": "480px"},
                        children=[
                            dcc.Graph(id="bar-chart"),
                            html.Div(
                                id="breaker-panel",
                                style={
                                    "marginTop": "12px",
                                    "padding": "12px",
                                    "background": "#222",
                                    "borderRadius": "8px",
                                },
                            ),
                            dcc.Interval(
                                id="refresh", interval=REFRESH_MS, n_intervals=0
                            ),
                        ],
                    ),
                    html.Div(
                        style={
                            "width": "420px",
                            "padding": "12px",
                            "background": "#222",
                            "borderRadius": "8px",
                        },
                        children=[
                            html.H4("Simulate Analyzer Failures (toggle ON/OFF)"),
                            html.Div(
                                id="analyzers-controls", children=analyzer_controls
                            ),
                            html.Div(
                                id="state-status",
                                style={"marginTop": "6px", "color": "#aaa"},
                            ),
                            html.Hr(),
                            html.H4("Set Weights and Observe Distribution"),
                            html.Div(id="weights-div", children=weights_controls),
                            html.Button("Save Weights", id="save-btn", n_clicks=0),
                            html.Div(
                                id="save-status",
                                style={"marginTop": "8px", "color": "#aaa"},
                            ),
                        ],
                    ),
                ],
            ),
        ],
    )


APP.layout = serve_layout


@APP.callback(Output("bar-chart", "figure"), Input("refresh", "n_intervals"))
def update_chart(n_intervals: int):
    """
    Update the bar chart showing recent Graylog message counts for each analyzer.

    Trigger: periodic interval.
    Returns: Plotly Figure with counts over the configured WINDOW_SECS window.
    """
    message_counts = [graylog_count(analyzer_name) for analyzer_name in ANALYZERS]
    LOGGER.info("[CHART] counts=%s (last %ss)", message_counts, WINDOW_SECS)
    figure = go.Figure(
        data=[
            go.Bar(
                x=ANALYZERS, y=message_counts, text=message_counts, textposition="auto"
            )
        ]
    )
    figure.update_layout(
        template="plotly_dark",
        yaxis_title=f"events in last {WINDOW_SECS }s",
        xaxis_title="analyzer",
        margin=dict(l=40, r=20, t=40, b=40),
        height=420,
    )
    return figure


@APP.callback(Output("breaker-panel", "children"), Input("refresh", "n_intervals"))
def update_breakers(n_intervals: int):
    """
    Refresh the circuit-breaker panel from distributor /health and render a centered table.
    Adds back the original detailed help text for interpreting breaker state.
    """
    breaker_status_map = fetch_breakers()
    header_block = html.Div(
        [
            html.Div(
                "Circuit Breakers", style={"fontWeight": "700", "marginBottom": "8px"}
            ),
            html.Div(
                [
                    "Live view of distributor /health — updates every refresh interval.",
                    html.Br(),
                    "• State = current breaker mode for this analyzer (closed=open for traffic, open=blocked, half_open=trial mode).",
                    html.Br(),
                    "• Failures = consecutive recent failures recorded by this breaker.",
                    html.Br(),
                    "• Reopen In = seconds until the next half-open probe is allowed (0 if already eligible).",
                    html.Br(),
                    "State is based on a single Gunicorn worker.",
                ],
                style={"color": "#aaa", "fontSize": "12px", "marginBottom": "8px"},
            ),
        ]
    )
    table = render_breaker_table(breaker_status_map)
    return html.Div([header_block, table])


@APP.callback(
    Output("state-status", "children"),
    inputs=[Input(f"state-{analyzer_name}", "value") for analyzer_name in ANALYZERS],
    prevent_initial_call=True,
)
def on_state_change(*radio_values: str):
    """
    Persist analyzer ON/OFF state changes to MongoDB and show a concise status message.

    Trigger: any analyzer toggle change.
    Returns: "Analyzer X was changed to ON|OFF" or a list if multiple changed.
    """
    try:
        # Incoming UI state -> {analyzer: bool}
        incoming_states = dict(
            zip(ANALYZERS, (value == "on" for value in radio_values))
        )

        # Current Mongo state (default to True if missing)
        mongo_states_now = {
            doc["name"]: bool(doc.get("active", True))
            for doc in ANALYZERS_COL.find({"name": {"$in": ANALYZERS}})
        }
        for analyzer_name in ANALYZERS:
            mongo_states_now.setdefault(analyzer_name, True)

        # Diff to find only the toggles that changed
        changed_pairs = [
            (analyzer_name, incoming_states[analyzer_name])
            for analyzer_name in ANALYZERS
            if incoming_states.get(analyzer_name) != mongo_states_now.get(analyzer_name)
        ]

        # Apply only the changes
        for analyzer_name, is_active in changed_pairs:
            ANALYZERS_COL.update_one(
                {"name": analyzer_name},
                {"$set": {"active": bool(is_active)}},
                upsert=True,
            )

        if not changed_pairs:
            return "No analyzer state change detected."

        if len(changed_pairs) == 1:
            single_name, single_val = changed_pairs[0]
            return (
                f"Analyzer {single_name} was changed to {'ON' if single_val else 'OFF'}"
            )

        return "; ".join(
            f"Analyzer {name} was changed to {'ON' if is_on else 'OFF'}"
            for name, is_on in changed_pairs
        )

    except PyMongoError as error:
        return f"Error writing states: {error}"


@APP.callback(
    Output("save-status", "children"),
    Input("save-btn", "n_clicks"),
    State("w-analyzer1", "value"),
    State("w-analyzer2", "value"),
    State("w-analyzer3", "value"),
    State("w-analyzer4", "value"),
    prevent_initial_call=True,
)
def save_config(_clicks: int, w1: float, w2: float, w3: float, w4: float):
    """
    Persist the weight configuration to MongoDB when the Save button is clicked.

    Trigger: Save Weights button.
    Returns: Success or error message.
    """
    try:
        LOGGER.info("Saving weights: %s, %s, %s, %s", w1, w2, w3, w4)
        WEIGHTS_COL.update_one(
            {"_id": "weights"},
            {
                "$set": {
                    "values": {
                        "analyzer1": float(w1 or 0),
                        "analyzer2": float(w2 or 0),
                        "analyzer3": float(w3 or 0),
                        "analyzer4": float(w4 or 0),
                    }
                }
            },
            upsert=True,
        )
        return "Weights saved."
    except (PyMongoError, ValueError) as error:
        return f"Error saving weights: {error}"


if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=8080, debug=False)
