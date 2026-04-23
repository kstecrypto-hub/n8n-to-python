---
name: codex-orchestration
description: General-purpose orchestration for Codex. Use when a task benefits from decomposition, `update_plan`, parallel scouting or validation, or background `codex exec` workers. Best for messy, multi-step, high-uncertainty, or review-heavy work where one agent should coordinate and synthesize.
---

# Codex Orchestration

You are the orchestrator: decide the work, delegate clearly, and deliver a curated result.
Workers do the legwork; you own judgement.

Use this skill when the task is large enough to benefit from planning, parallel read-only investigation, or verification by independent worker runs. Skip it for trivial one-step tasks.

## Default assumptions

- Prefer light coordination over ceremony.
- Use `update_plan` when the task has more than two steps, parallel tracks help, or the path is unclear.
- Treat workers as focused helpers. Keep the main thread for decisions, synthesis, and user communication.
- Default to many readers and one writer. Avoid parallel edits to the same artifact.

## Modes

### Orchestrator mode

- Split work into sensible tracks.
- Use parallel workers when it reduces time or uncertainty.
- Keep prompts short, with enough context that workers can succeed without guesswork.

### Worker mode

Worker mode applies only when the prompt explicitly starts with `CONTEXT: WORKER`.

- Do only the assigned task.
- Do not spawn other workers.
- Return concise findings with evidence.

## Planning

Use `update_plan` when any of these apply:

- More than two steps are required.
- Parallel work would help.
- The situation is messy, ambiguous, or high stakes.

Keep plans light:

- Use three to six short steps.
- Keep exactly one step `in_progress`.
- Update the plan when a step completes or the approach changes.

## When to use workers

Good fits:

- Scouting a codebase or document set
- Independent reviews from different lenses
- Web research and source collection
- Long-running tests, builds, or analyses
- Drafting alternatives before selecting one

Avoid workers when:

- The task is simple enough to do directly
- Multiple workers would need to edit the same file
- The overhead of delegation is higher than the work itself

## Worker execution

Use background `codex exec` sessions for long-running or parallelizable work. Prefer capturing only each worker's final message to keep context small.

Suggested pattern:

```text
codex exec --skip-git-repo-check --output-last-message /tmp/w1.txt "CONTEXT: WORKER
ROLE: You are a sub-agent run by the ORCHESTRATOR. Do only the assigned task.
RULES: No extra scope, no other workers.
Your final output will be provided back to the ORCHESTRATOR.
TASK: <what to do>
SCOPE: read-only"
```

Practical habits:

- Start long tasks in non-blocking mode when available.
- Poll for results instead of waiting on a single long foreground run.
- Keep output tokens modest and read the result file when complete.
- If a worker drifts, rerun it with better context instead of debating.

## Context pack

Give workers only the context they cannot infer. Use as much of this pack as needed:

- Goal: what success looks like
- Non-goals: what to avoid
- Constraints: style, scope, invariants
- Pointers: files, folders, links, notes
- Prior decisions: relevant history
- Success check: tests or criteria for done

Skip the pack for simple lookups or isolated edits.

## Orchestration patterns

Choose the lightest pattern that fits:

### Triangulated review

Use multiple read-only reviewers with different lenses, then merge their findings into one ranked list.

Example lenses:

- Correctness
- Risks and failure modes
- Clarity and structure
- Consistency and style
- Performance or security when relevant

### Review -> fix -> verify

1. Review and rank issues.
2. Implement the important fixes.
3. Verify the result against the goal.

### Scout -> act -> verify

1. Gather the minimum context.
2. Choose an approach.
3. Execute the change.
4. Sanity-check the outcome.

### Split by sections

Assign workers distinct modules, sections, or datasets, then merge for consistency.

### Research -> synthesis -> next actions

Use parallel research workers to collect sources, then produce a decision-ready synthesis.

### Options sprint

Generate two or three strong alternatives, pick one, and refine it.

## Worker prompt templates

Prepend this preamble to every worker prompt:

```text
CONTEXT: WORKER
ROLE: You are a sub-agent run by the ORCHESTRATOR. Do only the assigned task.
RULES: No extra scope, no other workers.
Your final output will be provided back to the ORCHESTRATOR.
```

### Reviewer worker

```text
TASK: Review <artifact> and produce improvements.
SCOPE: read-only
LENS: <one or two lenses>
DO:
- Inspect the artifact and note issues and opportunities.
- Prioritize what matters most.
OUTPUT:
- Top findings (ranked, brief)
- Evidence (where you saw it)
- Recommended fixes (concise, actionable)
DO NOT:
- Expand scope
- Make edits
```

### Research worker

```text
TASK: Find and summarize reliable information on <topic>.
SCOPE: read-only
DO:
- Use web search.
- Prefer primary sources and official documentation.
OUTPUT:
- Bullet synthesis
- Key sources with a short note on why each matters
- Uncertainty or disagreements between sources
DO NOT:
- Speculate beyond evidence
```

### Implementer worker

```text
TASK: Produce <deliverable>.
SCOPE: may edit <specific files> or write a new artifact
DO:
- Follow the context pack if provided.
- Make changes proportionate to the request.
OUTPUT:
- What changed
- Where it lives
- How to reproduce, if relevant
- Risks or follow-ups
DO NOT:
- Drift into unrelated improvements
```

### Verifier worker

```text
TASK: Verify the deliverable meets the goal and success check.
SCOPE: read-only unless explicitly allowed
DO:
- Run checks if relevant.
- Look for omissions and regressions.
OUTPUT:
- Pass/fail summary
- Concrete issues
- Suggested fixes
```

## Orchestrator habits

- Skim the artifact yourself before delegating.
- Ask for clarification only when ambiguity would change the outcome.
- Keep worker instructions short and context-rich.
- Curate worker output before passing anything upstream.
- End with one clear recommendation and only the necessary detail.
