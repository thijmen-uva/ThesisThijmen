# Communication pattern routing logic for multi-agent benchmarks

import random


def get_target_agents(agent_id: int, num_agents: int, pattern: str) -> list:
    """
    Determine which agents this agent should send messages to, based on the communication pattern.

    Args:
        agent_id: ID of the sending agent (0-indexed)
        num_agents: Total number of agents in the system
        pattern: Communication pattern ("pairwise", "random", "hotspot")

    Returns:
        List of agent IDs that should receive messages from this agent
    """

    if pattern == "pairwise":
        # Agents are organized into pairs: 0↔1, 2↔3, 4↔5, etc.
        if agent_id % 2 == 0:
            # Even agent sends to the next odd agent
            if agent_id + 1 < num_agents:
                return [agent_id + 1]
            else:
                # If odd agent doesn't exist, send to self (or no one)
                return []
        else:
            # Odd agent sends to the previous even agent
            return [agent_id - 1]

    elif pattern == "random":
        # Each agent sends to a randomly selected agent (different from itself)
        other_agents = [i for i in range(num_agents) if i != agent_id]
        if other_agents:
            return [random.choice(other_agents)]
        return []

    elif pattern == "hotspot":
        # 10% of agents are hotspots, 80% of traffic goes to them
        num_hotspots = max(1, num_agents // 10)  # 10% of agents

        # Determine hotspot agents (first 10%)
        hotspot_agents = list(range(num_hotspots))

        if agent_id in hotspot_agents:
            # Hotspot agents send to other hotspot agents
            other_hotspots = [h for h in hotspot_agents if h != agent_id]
            if other_hotspots:
                return [random.choice(other_hotspots)]
            return []
        else:
            # Non-hotspot agents: 80% send to hotspots, 20% send to random
            if random.random() < 0.8:
                # Send to hotspot
                return [random.choice(hotspot_agents)]
            else:
                # Send to random non-hotspot agent
                non_hotspots = [
                    i
                    for i in range(num_agents)
                    if i not in hotspot_agents and i != agent_id
                ]
                if non_hotspots:
                    return [random.choice(non_hotspots)]
                else:
                    # Fallback to hotspot if no non-hotspots available
                    return [random.choice(hotspot_agents)]

    else:
        raise ValueError(f"Unknown communication pattern: {pattern}")


def get_agent_topic(agent_id: int) -> str:
    """
    Get the inbox topic name for an agent.

    Args:
        agent_id: Agent ID (0-indexed)

    Returns:
        Topic name (e.g., "agent_0", "agent_1", etc.)
    """
    return f"agent_{agent_id}"


def get_run_agent_topic(run_id: str, agent_id: int) -> str:
    """
    Get a run-scoped topic name for an agent to avoid cross-run collisions.

    Args:
        run_id: Run identifier
        agent_id: Agent ID (0-indexed)

    Returns:
        Topic name (e.g., "<run_id>-agent-0")
    """
    return f"{run_id}-agent-{agent_id}"
