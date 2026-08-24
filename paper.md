# Goal of this paper

Focus: **skills used in GitHub Agentic Workflows for issue triage** — what makes them good, then **generate** triage skills / custom instructions that `gh aw compile` can bake into `.lock.yml`.

## Main question

What characteristics of Agent Skills make gh-aw **issue triage** effective, and can those characteristics be **generated and compiled**?

Break down:
- What does *good* mean for an **issue-triage** skill in gh-aw?
- Which metrics (label accuracy, override rate, time-to-triage, skill pickup stages)?
- What does it take to **build/generate** a good triage skill so compile installs it?

## Scope

- In: issue triage agentic workflows (classify, label/suggest, duplicates, spam, comments, project board, approval gates)
- Out of primary focus: CI-fix and generic fix-and-PR (may share lessons only)
- Skills: frontmatter `skills:`, inline `## skill:`, hint/fusion, paths like `.github/skills/`, `.agents/skills/`

## Pipeline

1. **Mine** public gh-aw triage workflows + attached skill packages
2. **Characterize** what distinguishes effective triage skills
3. **Generate** repo-specific `SKILL.md` (+ references / custom instructions)
4. **Compile** via `gh aw compile` so `.lock.yml` installs/activates the skill

## Generator → compile

Generator outputs:
- `.github/skills/issue-triage/SKILL.md` (+ `references/label-taxonomy.md`)
- optional prompt hints (load skill, stay on allowlist)
- frontmatter patch: `skills: [ .github/skills/issue-triage ]`

`gh aw compile` must then emit lock-file activation (`gh skill install`, skill on disk for Copilot). RQ6 success = configured → installed → available (and invoked/followed when the task warrants it).

## RQs

- **RQ1** How are triage gh-aw workflows structured, and how often do they attach skills vs inline prompts?
- **RQ2** What characteristics distinguish high-quality triage skills from weak / skill-free ones?
- **RQ3** Do length, specificity, taxonomy completeness, failure handling, modularity predict GitHub triage outcomes?
- **RQ4** How do people gate actions (auto-apply vs `issue-intent` vs second-agent comment review), and how should the generator encode that?
- **RQ5** Can generated skills/instructions beat inline-prompt baselines?
- **RQ6** Does compile produce a `.lock.yml` that actually installs and enables the generated skill?

## Outcome metrics (triage)

- Label accuracy vs maintainer-final labels
- Override / rejection rate of issue-intents
- Time-to-triage (`needs-triage` → triaged)
- False spam / false duplicate
- Workflow conclusion + five-stage pickup (configured → followed)

## Skill registration / discovery (keep for methods)

- Frontmatter `skills:` (preferred; compiler installs) — **this is what the generator must write**
- Inline `## skill:`, hint, fusion
- Copilot dirs: `.github/skills/`, `.claude/skills/`, `.agents/skills/` — catalog ≠ use
- Stages: Configured / Installed / Available / Invoked / Followed

## Notes from mining (pilot)

- Triage is a large share of gh-aw tasks; **skills are rare** among triage repos
- Approval: mostly auto-apply labels; few use `issue-intent: true` (cli/cli, copilot-sdk, primer-docs)
- Strong skill examples: cli/cli (suggest + taxonomy + spam), SkiaSharp (auto-apply + project board + SKILL.md)
