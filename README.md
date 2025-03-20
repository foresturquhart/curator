# Curator

Curator is a self-hosted application for managing image collections with advanced search, tagging, and organization capabilities.

## Overview

Curator is designed to help you organize, search, and browse your image collections with powerful features:

- **Vector-based similarity search**: Find visually similar images using CLIP neural embeddings
- **Intelligent deduplication**: Prevent duplicate uploads through content-based image hashing
- **Advanced tagging system**: Organize images with a flexible tagging system
- **People management**: Track creators and subjects in your images
- **Metadata enrichment**: Store titles, descriptions, and source information

## Project Status

⚠️ **This project is in early development**

This is a work-in-progress prototype, and many critical features are still missing or incomplete. The current codebase prioritizes functionality over perfect code quality as we validate core concepts.

### Current Limitations

- No authentication or multi-user support
- No user interface at this stage
- Missing features like collections and tag hierarchies
- Code needs refactoring and optimization
- No test coverage

## Technology Stack

- **Backend**: Go with Echo framework
- **Database**: PostgreSQL with pgvector extension
- **Search**: Elasticsearch for metadata, Qdrant for vector similarity
- **ML**: OpenAI CLIP for generating image embeddings

## Contributing

As this is an early prototype, we're not actively seeking contributions yet, but feel free to open issues for feature suggestions or bug reports.