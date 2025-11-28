#!/usr/bin/env bash

echo "Running start up script."

chmod +x /home/prospector/workspace/.devcontainer/on-start-host.sh && \
    /home/prospector/workspace/.devcontainer/on-start-host.sh
