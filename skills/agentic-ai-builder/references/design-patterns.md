# Design Patterns

## Opportunity Patterns

Good first agents:

- customer support resolution helpers
- internal operations workflows
- sales research and routing
- fraud or exception investigation copilots
- document processing pipelines
- structured follow-up and case handling

Poor first agents:

- vague “general employee replacement” concepts
- brittle workflows with no documented process
- high-risk actions with no rollback or escalation path

## Identity Pattern

Every agent needs a concrete identity block:

- role
- mission
- scope
- response style
- hard constraints
- output format

If the identity is vague, downstream behavior will be vague.

## Manager And Specialist Pattern

Use this when the top-level problem is a routing problem.

Manager agent responsibilities:

- inspect the request
- classify the request type
- invoke the correct specialist
- merge or format the final result

Specialist agent responsibilities:

- own one information domain or one action domain
- use a small toolset
- return a strict output contract

Good examples:

- person-info agent
- company-info agent
- document-summary agent
- approval-routing agent

## Tool Resilience Pattern

For every tool, assess:

- `control`: how much you control the tool or system
- `impact`: how critical the tool is to business success

Risk rule:

- high impact + low control requires fallback and graceful degradation
- high impact + high control deserves hardening and monitoring
- low impact tools can often fail soft

## Memory Pattern

Use:

- episodic memory for prior runs and cases
- semantic memory for facts, policies, and stable knowledge
- procedural memory for how-to steps and successful playbooks

Attach metadata for:

- case type
- user segment
- tool used
- outcome quality
- date
- source system

## Guardrail Pattern

Always include:

- explicit escalation conditions
- audit logs
- validation checks after major actions
- output formatting rules
- failure messages that are actionable

## Practical Build Heuristics

- One tool per agent is a strong default.
- Simpler systems are usually more reliable.
- Agents should verify their own work when possible.
- Integration friction is often harder than prompt design.
- Real deployment is harder than demo success.
- Build for recovery, not just for the happy path.

## Suggested Deliverable Shape

When you design an agentic system, return:

1. Use case and value.
2. Why it should or should not be agentic.
3. Agent or multi-agent topology.
4. Tool matrix.
5. Memory plan.
6. Guardrail and escalation plan.
7. Test cases.
8. Rollout phases.
