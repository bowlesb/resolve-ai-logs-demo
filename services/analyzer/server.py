"""
Analyzer gRPC Service

This module implements a lightweight analyzer process that:
  • Exposes a gRPC service (Analyzer.Analyze) for receiving batches of log messages.
  • Streams each message to Graylog via GELF UDP.
  • Polls MongoDB for an "active" flag that can enable/disable request handling at runtime.

"""

import os, time, threading, logging
from concurrent import futures
from datetime import datetime

import grpc
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from graypy import GELFUDPHandler

import logs_pb2
import logs_pb2_grpc

ANALYZER_NAME = os.environ.get("ANALYZER_NAME", "analyzer1")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017/")
GRAYLOG_HOST = os.environ.get("GRAYLOG_HOST", "graylog")
GRAYLOG_PORT = int(os.environ.get("GRAYLOG_PORT", "12201"))
POLL_SECS = 2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(GELFUDPHandler(GRAYLOG_HOST, GRAYLOG_PORT))

MONGO_CLIENT = MongoClient(MONGO_URI)
ANALYZERS_COL = MONGO_CLIENT.control.analyzers

active = True  # analyzer is by default "ON", unless it is explicitly turned "OFF" by Mongo state below.


def poll_active():
    global active  # TODO: avoid using global variable
    while True:
        try:
            doc = ANALYZERS_COL.find_one({"name": ANALYZER_NAME}) or {}
            active = bool(doc.get("active", True))
        except PyMongoError as e:
            logger.warning("Mongo check failed: %s", e)
        time.sleep(POLL_SECS)


class AnalyzerService(logs_pb2_grpc.AnalyzerServicer):
    def Analyze(self, request, context):
        if not active:
            context.abort(grpc.StatusCode.UNAVAILABLE, f"{ANALYZER_NAME} inactive")

        # Trivial processing: just log each message with extra str to Graylog
        for msg in request.messages:
            logger.info(f"{ANALYZER_NAME}: {msg.message} - I was analyzed!")

        # Note: web app will search by ANALYZER_NAME prefix.
        # Punting on a more resilent way of storing / searching logs for now

        return logs_pb2.Ack(
            accepted=True, note=f"{ANALYZER_NAME} accepted {len(request.messages)} msgs"
        )


def serve():
    threading.Thread(target=poll_active, daemon=True).start()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    logs_pb2_grpc.add_AnalyzerServicer_to_server(AnalyzerService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
