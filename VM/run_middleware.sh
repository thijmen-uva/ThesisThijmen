#!/usr/bin/env bash
set -e

########################################
# 参数解析
########################################

MIDDLEWARE=$1
MODE=$2   # best-effort | reliable

if [ -z "$MIDDLEWARE" ] || [ -z "$MODE" ]; then
  echo "Usage: $0 {kafka|mqtt|zenoh} {best-effort|reliable}"
  exit 1
fi

if [[ "$MODE" != "best-effort" && "$MODE" != "reliable" ]]; then
  echo "[ERROR] MODE must be 'best-effort' or 'reliable'"
  exit 1
fi

########################################
# 通用配置（可覆盖）
########################################

CPUS=${CPUS:-2}
MEMORY=${MEMORY:-4g}
CPUSET=${CPUSET:-""}   # 例如 "0-1"

# Kafka
# IMPORTANT: advertised.listeners must be reachable from the client machine.
# If KAFKA_NODE is not set explicitly, infer VM primary IP instead of localhost.
# For external VM deployments, set KAFKA_NODE to public IP or DNS before running this script.
KAFKA_NODE=${KAFKA_NODE:-$(hostname -I | awk '{print $1}')}
KAFKA_CONTROLLER_NODE=${KAFKA_CONTROLLER_NODE:-127.0.0.1}
KAFKA_PORT=${KAFKA_PORT:-9092}
KAFKA_CONTROLLER_PORT=${KAFKA_CONTROLLER_PORT:-9093}
KAFKA_VERSION=${KAFKA_VERSION:-4.0.0-debian-12-r10}

# Keep client-side broker env aligned with advertised listener host.
KAFKA_BROKER_IP=${KAFKA_BROKER_IP:-${KAFKA_NODE}:${KAFKA_PORT}}

# MQTT
MQTT_PORT=${MQTT_PORT:-1883}

# Zenoh
ZENOH_PORT=${ZENOH_PORT:-7447}

########################################
# 日志
########################################

echo "========================================"
echo "[INFO] Middleware : $MIDDLEWARE"
echo "[INFO] Mode       : $MODE"
echo "[INFO] CPU        : $CPUS"
echo "[INFO] Memory     : $MEMORY"
echo "[INFO] Kafka node : $KAFKA_NODE"
echo "[INFO] Kafka addr : $KAFKA_BROKER_IP"
echo "[INFO] Kafka ctrl : $KAFKA_CONTROLLER_NODE:$KAFKA_CONTROLLER_PORT"
echo "========================================"

########################################
# 清理环境
########################################

echo "[INFO] Cleaning existing containers..."
docker rm -f kafka mqtt zenoh 2>/dev/null || true

# （可选）更严格实验
# docker system prune -f

########################################
# Docker 资源参数
########################################

DOCKER_RES_ARGS="--cpus=$CPUS --memory=$MEMORY"

if [ -n "$CPUSET" ]; then
  DOCKER_RES_ARGS="$DOCKER_RES_ARGS --cpuset-cpus=$CPUSET"
fi

########################################
# 语义提示（不会直接影响 broker）
########################################

if [ "$MODE" = "best-effort" ]; then
  KAFKA_ACKS=0
  MQTT_QOS=0
  ZENOH_RELIABILITY="best_effort"
else
  KAFKA_ACKS=all
  MQTT_QOS=1
  ZENOH_RELIABILITY="reliable"
fi

echo "[INFO] Semantic alignment (client-side):"
echo "       Kafka ACKS   = $KAFKA_ACKS"
echo "       MQTT QoS     = $MQTT_QOS"
echo "       Zenoh mode   = $ZENOH_RELIABILITY"

########################################
# 启动中间件
########################################

case "$MIDDLEWARE" in

  kafka)
    echo "[INFO] Starting Kafka..."

    docker run -d \
      --name kafka \
      $DOCKER_RES_ARGS \
      -p "${KAFKA_PORT}:${KAFKA_PORT}" \
      -p "${KAFKA_CONTROLLER_PORT}:${KAFKA_CONTROLLER_PORT}" \
      -e KAFKA_ENABLE_KRAFT=yes \
      -e KAFKA_CFG_NODE_ID=1 \
      -e KAFKA_CFG_PROCESS_ROLES="controller,broker" \
      -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS="1@${KAFKA_CONTROLLER_NODE}:${KAFKA_CONTROLLER_PORT}" \
      -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES="CONTROLLER" \
      -e KAFKA_CFG_LISTENERS="PLAINTEXT://:${KAFKA_PORT},CONTROLLER://:${KAFKA_CONTROLLER_PORT}" \
      -e KAFKA_CFG_ADVERTISED_LISTENERS="PLAINTEXT://${KAFKA_NODE}:${KAFKA_PORT}" \
      -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" \
      -e KAFKA_CFG_INTER_BROKER_LISTENER_NAME="PLAINTEXT" \
      -e KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
      -e KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
      -e KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=1 \
      -e ALLOW_PLAINTEXT_LISTENER=yes \
      -e EXP_MODE="$MODE" \
      bitnamilegacy/kafka:"${KAFKA_VERSION}"

    echo "[INFO] Waiting for Kafka to be ready..."
    sleep 10
    ;;

  mqtt)
    echo "[INFO] Starting MQTT (Mosquitto)..."

    docker run -d \
      --name mqtt \
      $DOCKER_RES_ARGS \
      -p "${MQTT_PORT}:${MQTT_PORT}" \
      -e EXP_MODE="$MODE" \
      eclipse-mosquitto

    sleep 3
    ;;

  zenoh)
    echo "[INFO] Starting Zenoh..."

    docker run -d \
      --name zenoh \
      $DOCKER_RES_ARGS \
      -p "${ZENOH_PORT}:${ZENOH_PORT}" \
      -e EXP_MODE="$MODE" \
      eclipse/zenoh

    sleep 3
    ;;

  *)
    echo "[ERROR] Unknown middleware: $MIDDLEWARE"
    exit 1
    ;;
esac

########################################
# 重要提醒
########################################

echo "----------------------------------------"
echo "[WARNING] Reliability is enforced at CLIENT side!"
echo "[INFO] Use the following settings in your client:"
echo "       export KAFKA_BROKER_IP=$KAFKA_BROKER_IP"
echo "       Kafka  -> acks=$KAFKA_ACKS"
echo "       MQTT   -> qos=$MQTT_QOS"
echo "       Zenoh  -> reliability=$ZENOH_RELIABILITY"
echo "----------------------------------------"

echo "[INFO] $MIDDLEWARE started successfully in $MODE mode."