# Agent Playbook

This playbook distills the provided book into practical instructions for building agents.

## 1. Start With Opportunity Selection

An agent is justified when:

- the task consumes meaningful recurring time
- freeing that time creates strategic value
- errors are costly or frequent
- the task scales across many cases
- the process is already reasonably standardized
- the required data and system access are available

Avoid recommending an agent when:

- the process is mostly ad hoc
- success depends on deep human judgment or emotional nuance
- inputs and outputs are too inconsistent to standardize
- the business value is weak relative to integration effort

## 2. Design With SPAR

Use the SPAR frame as the minimum architecture vocabulary:

- `Sensing`: what the agent observes
  Examples: inbound message, form payload, CRM record, ticket text, file contents, telemetry
- `Planning`: how the agent decides
  Examples: classify request, choose a workflow branch, decide whether to call a sub-agent, ask for approval
- `Acting`: what the agent does
  Examples: call APIs, search, summarize, update records, send messages, create documents
- `Reflecting`: how the agent checks and improves
  Examples: verify outcome, compare expected vs actual result, inspect tool errors, collect user feedback

Do not leave reflection implicit. Agents that act without verification degrade quickly in production.

## 3. Define The Agent Boundary

For each agent, specify:

- purpose
- owned task
- inputs
- outputs
- allowed tools
- forbidden actions
- escalation triggers
- success criteria

Boundaries matter more than clever prompts.

## 4. Prefer Minimal Structures

Default patterns:

- `single agent`: use when one workflow with one or two tools is enough
- `manager + specialists`: use when the top-level task splits cleanly into stable subtasks
- `pipeline workflow`: use when the sequence is deterministic and handoffs are explicit

Avoid unnecessary sub-agents.
Only split when each specialist has a narrow responsibility and a clear handoff contract.

## 5. Action Design

When an agent takes action:

- make tool dependencies explicit
- record prerequisite relationships between steps
- define fallback tools or alternate paths for high-impact dependencies
- classify tools by control and impact

High-impact, low-control tools need fallback plans.

Examples of action design fields:

- `step_id`
- `action`
- `tool`
- `expected_outcome`
- `dependency`
- `fallback`

## 6. Reasoning Design

Use the lightest reasoning mechanism that fits the task.

- Use standard LLM reasoning for summarization, extraction, routing, and conversational synthesis.
- Use stronger reasoning models or constrained logic for multi-step analysis, math, coding, or technical decisions.
- When correctness matters, require intermediate checks instead of trusting a single free-form completion.

Do not overspend on reasoning depth for simple workflow steps.

## 7. Memory Design

Think in three layers:

- `short-term memory`: current task context and active thread state
- `long-term memory`: episodic, semantic, and procedural memory
- `feedback loop`: logs, user feedback, outcome metrics, and adaptation signals

Common mapping:

- relational DB for structured state
- document or NoSQL store for flexible records
- vector store for semantic retrieval
- graph store for relationships when multi-hop retrieval matters

Use metadata and tagging to avoid retrieval chaos.

## 8. Rules, Guardrails, And Trust

Before deployment, define:

- filtering and prioritization rules
- allowed and denied tools
- rate limits
- circuit breakers
- timeout handling
- human escalation paths
- decision logging
- redaction or privacy rules

Use progressive trust:

- high oversight in early rollout
- reduced human intervention only after reliability is proven

## 9. Implementation And Rollout

Implementation guidance:

- start with a working baseline, not a perfect platform search
- test on real scenarios, not toy prompts
- integrate where users already work
- standardize input and output formats
- separate process data from side effects
- plan for multiple refinement cycles

Rollout guidance:

- start with the smallest valuable component
- monitor outcomes and failures
- expand scope only after stable operation
- allocate serious time to integration and deployment, not just prototype building
