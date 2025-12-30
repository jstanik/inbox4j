[![Build Status](https://github.com/jstanik/inbox4j/actions/workflows/maven-build.yaml/badge.svg)](
https://github.com/jstanik/inbox4j/actions/workflows/maven-build.yaml
)
# Inbox4j
WORK IN PROGRESS

**Inbox4j** is a Java library that implements the Inbox Pattern to provide reliable, ordered, and scalable message processing.

The core idea behind inbox4j is simple: persist incoming messages to a database first, then process them in a controlled and fault-tolerant way.
By storing messages durably before handling them, inbox4j protects your system from message loss, duplicate processing, and ordering issues—especially in distributed or event-driven architectures.

A key feature of inbox4j is its semi-parallel processing model.
Messages are processed in parallel across different recipients, while preserving strict order for messages that belong to the same recipient.
This allows you to scale throughput without sacrificing consistency where ordering matters.

Visit [https://github.com/jstanik/inbox4j](https://github.com/jstanik/inbox4j).
