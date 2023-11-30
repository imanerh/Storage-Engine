# Storage-Engine

## Introduction

This repository contains the final project for the UM6P Introduction to Golang course. The project aims to implement a Persistent Key-Value Store in Go, leveraging our recent learning of various language features, concurrency, and HTTP handling.

## Project Overview

The goal is to build a Persistent Key-Value Store that exposes simple HTTP endpoints for data manipulation. The architecture follows the Log-Structured Merge Tree (LSM tree) model for efficient reading and writing of data.

### Functionality

The key components of the project include:

- **HTTP API Endpoints:**
  - `GET /get?key=keyName`: Retrieve the value associated with the specified key or indicate 'Key not found'.
  - `POST /set`: Set a key-value pair provided in the request body (using JSON encoding).
  - `DELETE /del?key=keyName`: Delete a key from the store and return the existing value if present.

- **Memtable and Write Ahead Log (WAL):**
  All write operations are stored in a memtable (sorted map) and appended to the Write Ahead Log (WAL) to ensure data durability in case of crashes.

- **SST File Storage:**
  Periodically, memtable contents are flushed to disk as an SST file (Sorted String Table) to maintain a snapshot of the memtable on disk.
