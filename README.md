#### UNIVERSITY OF AMSTERDAM

# Experimental Plan

## Mengbing Zhou

```
April 15, 2026
```
## 1 OBJECTIVE

The goal of this study is to systematically evaluate the performance characteristics of different
communication middleware systems, including Kafka, MQTT, and Zenoh, under DT-inspired
multi-agent communication workloads. We focus on latency, throughput, scalability, and
robustness under varying workload patterns.

## 2 SYSTEM SETUP

### 2.1 DEPLOYMENT ARCHITECTURE

The experimental setup consists of two components:

- **Agent Simulator** : A large number of lightweight agents are simulated on a local ma-
    chine using asynchronous processes.
- **Middleware Node** : The communication middleware (Kafka / MQTT / Zenoh) is de-
    ployed as a containerized service on a dedicated VM.

All agents communicate exclusively through the middleware.

### 2.2 UNIFIED AGENT MODEL

Each agent is modeled as a message-driven entity:

- Each agent _Ai_ is associated with an inbox topic _Ti_
- _Ai_ consumes messages from _Ti_


- _Ai_ produces messages to other agents’ topics

This abstraction enables middleware-focused evaluation independent of application seman-
tics.

## 3 WORKLOAD DESIGN

### 3.1 MESSAGE TYPES AND WORKLOAD COMPOSITION

To reflect realistic communication patterns in multi-agent digital twin systems, we model
message heterogeneity using a set of predefined message types with different payload sizes.
Instead of using a single fixed message size, each message is sampled from this type set ac-
cording to a configurable probability distribution.

#### MESSAGE TYPES.

- **Type A (Control)** : 64 Bytes
- **Type B (State Update)** : 1 KB
- **Type C (Data Payload)** : 10 KB
- **Type D (Large Payload)** : 100 KB

WORKLOADCOMPOSITION. We define the workload as a probability distribution over mes-
sage types:
_P_ ( _TA_ ), _P_ ( _TB_ ), _P_ ( _TC_ ), _P_ ( _TD_ )

Based on this, we consider the following representative workload compositions:

- **Uniform (Baseline):**

```
P ( TA )= 25%, P ( TB )= 25%, P ( TC )= 25%, P ( TD )= 25%
```
- **Lightweight-dominant (DT-realistic):**

```
P ( TA )= 60%, P ( TB )= 25%, P ( TC )= 10%, P ( TD )= 5%
```
- **Heavy-tail (Bandwidth-stress):**

```
P ( TA )= 70%, P ( TB )= 15%, P ( TC )= 10%, P ( TD )= 5%
```
This probabilistic formulation enables us to systematically evaluate middleware performance
under varying workload compositions, ranging from uniform traffic to control-dominated
and bandwidth-stressing scenarios.


DEFAULTCONFIGURATION. Since the experimental design involves multiple controllable di-
mensions (e.g., workload intensity, generation pattern, concurrency level, and message type
distribution), exploring all combinations would lead to an excessively large experimental
space.
To ensure clarity and focus, we adopt the **lightweight-dominant** distribution as the default
configuration in most experiments, as it best reflects typical communication patterns in dig-
ital twin systems, where control and state update messages dominate.
Other workload compositions (e.g., uniform and heavy-tail) could be evaluated separately
in dedicated experiments to analyze the sensitivity of middleware performance to message
heterogeneity.

### 3.2 COMMUNICATION PATTERNS

1. PAIRWISECOMMUNICATION (BASELINE) Agents are organized into pairs. Each pair ex-
changes messages bidirectionally:
_Ai_ ↔ _Aj_

This pattern provides a controlled environment without contention.

2. RANDOMCOMMUNICATIONEach agent sends messages to randomly selected target agents:

```
Ai → Aj , j ∼ Uniform( A )
```
This models unstructured interactions in large-scale systems.

3. HOTSPOTCOMMUNICATIONA small subset of agents receives the majority of messages:
    - 10% of agents act as hotspots
    - 80% of traffic is directed to them

This reflects skewed workloads in real DT systems.

### 3.3 MESSAGE GENERATION PATTERNS AND WORKLOAD INTENSITY

We model message generation as a combination of _generation patterns_ and _workload inten-
sity_. The generation pattern defines the temporal characteristics of message arrivals, while
the workload intensity controls the average message rate.

```
WORKLOADINTENSITY (RATE). We consider three levels of message generation rate per agent:
```
- Low: 10 msg/s
- Medium: 100 msg/s
- High: 500 msg/s


MESSAGEGENERATIONPATTERNS. Based on the workload intensity, we define the following
generation patterns:

- **Constant Rate** : Messages are generated at a fixed rate, resulting in a steady and pre-
    dictable workload. This serves as the baseline scenario for controlled performance
    evaluation.
- **Poisson Process** : Messages follow a Poisson arrival process with rate _λ_ , modeling event-
    driven behaviors commonly observed in digital twin systems. This introduces natural
    variability in inter-arrival times.
- **Burst Traffic** : The system alternates between normal and burst phases. During burst
    periods, the generation rate increases (e.g., 10× the baseline rate) for a short duration,
    simulating sudden workload spikes.

In all cases, message generation is subject to the in-flight constraint _k_ , which may introduce
backpressure when the system becomes saturated.

### 3.4 CONCURRENCY CONTROL

Each agent maintains a configurable upper bound on the number of _in-flight messages_ , de-
noted by _k_ :
_k_ ∈ {1, 5, 10}

An in-flight message is defined as a message that has been sent but not yet _consumed_ by the
receiver. In this work, we adopt a **consumption-based completion model** , where a message
is considered completed once it is received and processed by the target agent. This design
avoids introducing additional acknowledgment traffic over the middleware and ensures that
the evaluation focuses exclusively on communication performance.

LOCALSHAREDCOUNTER-BASEDCOMPLETION. To implement in-flight tracking efficiently,
we adopt a **local shared counter mechanism**. Each agent maintains a thread-safe counter
that records the number of outstanding in-flight messages. The counter is updated locally
within the simulation environment, without generating additional network messages.
Operationally, the mechanism works as follows:

- When a message is generated and successfully dispatched to the middleware, the local
    in-flight counter is incremented.
- When the corresponding message is consumed by the receiver agent, a local comple-
    tion signal is triggered within the simulation runtime (i.e., within the agent framework),
    and the sender’s in-flight counter is decremented.
- No explicit acknowledgment message is transmitted via the middleware; completion
    tracking is handled entirely by the local simulation layer.


This implementation ensures that the middleware is not burdened with additional control
messages, and the measured performance reflects only data-plane communication behavior.
The parameter _k_ controls the _concurrency level_ (i.e., pipeline depth) of each agent, while the
generation rate controls the _workload intensity_. Their interaction determines whether the
system operates in an unsaturated regime (rate × latency < _k_ ) or a saturated regime (rate
× latency≥ _k_ ), which is critical for evaluating middleware performance under varying load
conditions.

### 3.5 SYSTEM SCALE

- Number of agents: 10, 50, 100, 500, 1000

### 3.6 MIDDLEWARE CONFIGURATION

To ensure a fair and meaningful comparison, we align different middleware systems under
comparable _reliability semantics_ , distinguishing between **best-effort** and **reliable** delivery
modes. Table 3.1 summarizes the corresponding configurations.

```
Table 3.1: Aligned middleware configurations under different reliability semantics
Semantic Kafka MQTTZenoh
Best-effort acks=1 QoS 0 best-effort
Reliable acks=all QoS 1 / QoS 2 reliable
```
All middleware systems are configured using their default settings beyond the reliability-
related parameters to ensure consistency and avoid system-specific optimizations that could
bias the comparison. This alignment ensures that observed performance differences are
attributable to inherent middleware design rather than mismatched delivery guarantees or
configuration tuning.

## 4 EVALUATION METRICS

### 4.1 LATENCY

- End-to-end latency (p50, p95, p99)

### 4.2 THROUGHPUT

- Messages per second
- Data throughput (MB/s)

### 4.3 RELIABILITY

- Message loss rate


### 4.4 STABILITY

- Latency variance (jitter)

### 4.5 RESOURCEUSAGE (OPTIONAL)

- CPU utilization
- Memory usage

## 5 DISCUSSION GOALS

The experimental results aim to answer the following questions:

- How do different middleware systems perform under increasing scale?
- What are the trade-offs between latency and throughput?
- How does workload skew (hotspot) affect performance?
- Which middleware is most suitable for large-scale DT systems?


