import os
import io
import logging
import threading
from concurrent import futures
from http.server import BaseHTTPRequestHandler, HTTPServer

import grpc
import torch
import clip
from PIL import Image

# Import the generated gRPC classes.
import clip_pb2
import clip_pb2_grpc

# Configure logging settings based on environment variable.
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level, logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load the CLIP model and preprocessing function; detect available device.
device = "cuda" if torch.cuda.is_available() else "cpu"
logger.debug(f"Loading CLIP model on {device}")
model, preprocess = clip.load("ViT-B/32", device=device)
logger.debug("CLIP model loaded successfully.")

class CLIPServiceServicer(clip_pb2_grpc.CLIPServiceServicer):
    """
    Implements the gRPC CLIP service for image embedding.
    """
    def GetImageEmbedding(self, request, context):
        logger.info("Received image embedding request")
        try:
            # Read the image from request bytes.
            image = Image.open(io.BytesIO(request.image_data))
            logger.debug(f"Image opened successfully; mode: {image.mode}")
        except Exception as e:
            logger.error(f"Error reading image: {e}")
            context.set_details(f"Error reading image: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return clip_pb2.EmbeddingResponse()

        # Ensure the image is in RGB format (convert if necessary).
        if image.mode != "RGB":
            logger.debug("Converting image to RGB")
            image = image.convert("RGB")

        # Preprocess the image for the CLIP model.
        logger.debug("Preprocessing image")
        processed_image = preprocess(image).unsqueeze(0).to(device)

        # Encode the image to obtain its embedding.
        logger.debug("Encoding image with CLIP model")
        with torch.no_grad():
            embedding = model.encode_image(processed_image)

        # Remove the batch dimension and convert the tensor to a list.
        embedding_vector = embedding.squeeze(0).cpu().tolist()
        logger.info("Returning embedding vector")
        return clip_pb2.EmbeddingResponse(embedding=embedding_vector)

class HealthCheckHandler(BaseHTTPRequestHandler):
    """
    HTTP handler for health check endpoint.
    """
    def do_GET(self):
        # Respond with HTTP 200 OK for health check.
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        # Override default logging to keep the output clean.
        return

def run_healthcheck_server(port):
    """
    Starts a simple HTTP server for health checks.
    """
    server_address = ("", port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info(f"Starting HTTP health check server on port {port}...")
    httpd.serve_forever()

def serve():
    """
    Initializes and starts both the gRPC server and the HTTP health check server.
    """
    # Retrieve ports from environment variables.
    grpc_port = int(os.environ.get("GRPC_PORT", 50051))
    healthcheck_port = int(os.environ.get("HEALTHCHECK_PORT", 8080))

    # Start the health check server in a separate daemon thread.
    healthcheck_thread = threading.Thread(target=run_healthcheck_server, args=(healthcheck_port,))
    healthcheck_thread.daemon = True
    healthcheck_thread.start()

    # Set up the gRPC server.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    clip_pb2_grpc.add_CLIPServiceServicer_to_server(CLIPServiceServicer(), server)
    server_address = f"[::]:{grpc_port}"
    server.add_insecure_port(server_address)

    logger.info(f"Starting gRPC server on port {grpc_port}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
