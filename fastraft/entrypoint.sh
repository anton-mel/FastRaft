#!/bin/sh

# Use the downward API for Pod IP or default to a placeholder
# SELF_IP=${SELF_IP:-$(hostname -i)}

# Log values for debugging
echo "Self IP: $SELF_IP"
echo "Peers: $PEERS_IP"

# Run the application with resolved environment variables
exec ./app "ME=${SELF_IP}:5000" "PEERS=${PEERS_IP}"

