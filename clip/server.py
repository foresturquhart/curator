import os
import io
import logging
from concurrent import futures

import grpc
import torch
import clip
from PIL import Image

# Import the generated classes
import clip_pb2
import clip_pb2_grpc

# Set up logging configuration
log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level, logging.INFO)
logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load the CLIP model and its preprocessing function.
device = "cuda" if torch.cuda.is_available() else "cpu"
logger.debug(f"Loading CLIP model on {device}")
model, preprocess = clip.load("ViT-B/32", device=device)
logger.debug("CLIP model loaded successfully.")

class CLIPServiceServicer(clip_pb2_grpc.CLIPServiceServicer):
    def GetImageEmbedding(self, request, context):
        logger.info("Received image embedding request")
        # Read the image from the request bytes
        try:
            image = Image.open(io.BytesIO(request.image_data))
            logger.debug(f"Image opened successfully; mode: {image.mode}")
        except Exception as e:
            logger.error(f"Error reading image: {e}")
            context.set_details(f"Error reading image: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            return clip_pb2.EmbeddingResponse()

        # Ensure the image is in RGB format (converts grayscale to 3-channel RGB)
        if image.mode != "RGB":
            logger.debug("Converting image to RGB")
            image = image.convert("RGB")

        # Preprocess the image for CLIP
        logger.debug("Preprocessing image")
        processed_image = preprocess(image).unsqueeze(0).to(device)

        # Get the embedding from the model
        logger.debug("Encoding image with CLIP model")
        with torch.no_grad():
            embedding = model.encode_image(processed_image)

        # Remove the batch dimension and move to CPU
        embedding_vector = embedding.squeeze(0).cpu().tolist()

        logger.info("Returning embedding vector")

        # Return the embedding in the response message
        return clip_pb2.EmbeddingResponse(embedding=embedding_vector)

def serve():
    # Get port from environment variable or default to 8080
    port = int(os.environ.get("PORT", 8080))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    clip_pb2_grpc.add_CLIPServiceServicer_to_server(CLIPServiceServicer(), server)

    # Use the port from environment variable
    server_address = f"[::]:{port}"
    server.add_insecure_port(server_address)

    logger.info(f"Starting gRPC server on port {port}...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
