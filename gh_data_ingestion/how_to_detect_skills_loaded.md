# Identifying Skill Activation in GitHub Agentic Workflows (`gh-aw`)

This guide outlines how to programmatically or manually verify if a **GitHub Agentic Workflow** has successfully discovered, loaded, and executed a specified skill. 

---

## 1. Source Declaration (Static Check)
Verify that the skill is explicitly declared within the Markdown workflow file (located under `.github/workflows/*.md`). The `gh-aw` framework looks for skills in two structures:

### A. Frontmatter Declarations
The skill folder or external reference must be explicitly listed under the `skills` array attribute at the top of the file:
```yaml
---
on: workflow_dispatch
engine: copilot
skills:
  - .github/skills/my-custom-skill  # Local repository skill
  - owner/repo/path/to/skill@sha    # External public skill reference
---
```

### B. Inline Skill Blocks
If the skill is declared inline, it must use the proper header naming convention and frontmatter description block within the Markdown file:
```markdown
## skill: `my-inline-skill`
---
description: "Specific operational constraints and toolsets for the agent"
---
```

---

## 2. Compilation Step (Pre-Run Check)
When the markdown source is processed via `gh aw compile`, the framework evaluates all imported instructions and constructs a generated execution environment. 

* **Validation Action**: Open the resulting compiled `.lock.yml` file.
* **Target Step**: Look for the `setup` or environment interpolation steps. The pipeline must explicitly contain tasks routing or unpacking files into the active workspace directory (such as `.github/skills/`). If it is not listed in the generated YAML, it will be ignored during execution.

---

## 3. Engine Activation Log (Runtime Check)
When an agentic workflow executes, it captures its starting state as an artifact. To confirm the LLM actually received the skill instructions, download and extract the runtime artifact zip bundle.

* **Target File**: `activation/aw_info.json`
* **Verification Rule**: Search the JSON payload for the `"skills"` or `"system_prompts"` keys. If the skill was picked up, the compiled text content of that skill's `SKILL.md` or inline instructions will be injected directly into this snapshot.

---

## 4. Telemetry Log Execution (Execution Check)
If the specified skill injects custom behaviors, environment variables, or Model Context Protocol (MCP) server hooks, you can verify execution through the downstream logs:

* **`mcp-gateway-traffic.log`**: Check this log if the skill introduces custom tools. Ensure the specific tool JSON-RPC schemas defined by the skill show active calling traffic.
* **`agent-stdio.log`**: Search this file for phrases, keywords, or console outputs specific to your skill's instruction set. This confirms the agent's reasoning loop actively processed the skill constraints.
