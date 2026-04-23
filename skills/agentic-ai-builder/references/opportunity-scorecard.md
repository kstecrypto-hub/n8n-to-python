# Opportunity Scorecard

Use this scorecard before recommending implementation.

## Impact

Rate each from 1 to 5:

- `time_investment`: how much recurring time the workflow consumes
- `strategic_value`: how valuable the freed time is
- `error_reduction`: how much the agent could reduce costly mistakes
- `scalability`: how repeatable the workflow is across teams or cases

## Feasibility

Rate each from 1 to 5:

- `process_standardization`: how clearly the workflow is already defined
- `data_readiness`: how accessible and structured the needed inputs are
- `tool_access`: how available the required systems and APIs are
- `integration_difficulty`: reverse score; 5 means easy to integrate

## Risk

Rate each from 1 to 5:

- `decision_risk`: cost of a bad autonomous decision
- `privacy_risk`: sensitivity of data handled
- `rollback_strength`: reverse score; 5 means strong rollback and auditability
- `human_override`: reverse score; 5 means easy for humans to intervene

## Decision Rule

- Strong candidate: high impact, solid feasibility, manageable risk
- Pilot candidate: high impact, medium feasibility, needs guardrails
- Not ready: low standardization, poor data readiness, or unacceptable risk

## Output Format

Return:

- numeric score table
- short justification per category
- final recommendation: `build now`, `pilot first`, or `do not automate yet`
