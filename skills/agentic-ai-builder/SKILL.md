---
name: agentic-ai-builder
description: Design, scope, and implement agentic AI systems using practical guidance for opportunity selection, SPAR-based agent design, action and tool use, reasoning depth, memory architecture, multi-agent orchestration, guardrails, testing, and rollout. Use when Codex needs to turn a business or personal workflow into a deployable AI agent or multi-agent system.
---

# Agentic AI Builder

Read `references/agent-playbook.md` first.

Read `references/design-patterns.md` when you need practical build heuristics, sub-agent structure, memory choices, or rollout guidance.

Read `references/opportunity-scorecard.md` before recommending that a workflow should become an agent.

Use `assets/agent-design-template.md` when the user wants a concrete agent specification, implementation plan, or architecture brief.

## Work In This Order

1. Confirm the use case is worth automating.
   Score time investment, strategic value of freed time, error reduction potential, scalability, process standardization, and data readiness.
2. Define the agent boundary.
   Specify what the agent owns, what stays human, what tools it can call, and what decisions require escalation.
3. Design with SPAR.
   Define sensing, planning, acting, and reflecting explicitly.
4. Keep the first version minimal.
   Prefer one agent with one tool or a small manager-plus-specialist pattern over a large autonomous graph of agents.
5. Separate process data from actions.
   Keep memory, state, and knowledge stores distinct from tool execution and side effects.
6. Build guardrails before rollout.
   Add logging, decision trails, rate limits, circuit breakers, fallback paths, and human approval where risk is non-trivial.
7. Test with real scenarios.
   Include edge cases, tool failures, ambiguous inputs, and recovery paths.
8. Ship progressively.
   Start with staged trust, monitor outcomes, then expand autonomy only after reliable performance.

## Default Rules

- Think in tasks, not job titles.
- Do not automate a process that has not already worked manually.
- Start with documented processes, examples, and explicit success criteria.
- Prefer augmentation over replacement when judgment, creativity, or emotional nuance is central.
- Favor simple agents over clever ones.
- Use sub-agents only when responsibilities are truly separable.
- Give each agent a precise identity, clear inputs, strict output format, and bounded toolset.
- Require agents to inspect the result of their own actions when possible.
- Log every important decision and tool call.
- Treat deployment and integration as first-class work, not cleanup after the prototype.

## Output Expectations

When asked to design an agent, produce:

1. Opportunity assessment.
2. Agent goal, scope, and non-goals.
3. SPAR design.
4. Tools, memory, and system integrations.
5. Guardrails and human-in-the-loop controls.
6. Test plan.
7. Rollout plan.

When asked to implement, keep the same structure and turn it into working artifacts.
