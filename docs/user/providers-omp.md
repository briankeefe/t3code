# OMP (Oh My Pi)

T3 Code connects to your existing [Oh My Pi](https://omp.sh) installation. OMP is a separate
provider from Pi, with its own RPC protocol, configuration, authentication, and sessions.
Changing Pi's binary path to `omp` is not a supported substitute.

## Set up OMP

1. Install OMP on the machine running the T3 Code server using the
   [upstream installation guide](https://github.com/can1357/oh-my-pi#install).
2. Run `omp` in a terminal on that machine and use `/login`, or configure the API key for your
   model provider. Complete any first-run or project trust prompts in the terminal.
3. Open **Settings → Providers**, enable **OMP**, and refresh its model catalog.

For a Bun installation, upstream documents:

```sh
bun install -g @oh-my-pi/pi-coding-agent
```

If `omp` is not on the server's `PATH`, set its **Binary path** to the executable. For a remote
server, installation and login happen there, not on the computer or phone displaying T3 Code.
You can also add your model provider's API-key environment variable to the OMP instance in
T3 Code; mark secret values as sensitive.
Set `OMP_PROFILE` in the instance environment to use a named OMP profile for both model discovery
and sessions. Launch arguments cannot override T3 Code's mode, session, approval policy, or
profile selection; `--config` overlays and `--alias` are also rejected.

OMP stays disabled until you enable it. T3 Code does not install OMP or change your global OMP
configuration. Update it through the installer or package manager that owns your installation.
For example, update a Bun global installation with `bun install -g @oh-my-pi/pi-coding-agent`.

## Models and commands

Refresh discovers OMP's usable models without starting an agent or loading extensions.
**OMP default** uses the model from your OMP configuration; named models use their
`provider/model` identity. The thinking picker uses each model's reported effort options rather
than assuming Pi's supported levels. Add an extension-only model as a custom model ID if it does
not appear in the background catalog.

The composer shows the selected effort beside the model, or **Default** until OMP reports its
current level. Open the picker with **Cmd+Shift+E** on macOS or **Ctrl+Shift+E** elsewhere, then
use the arrow keys and Enter to choose a level. Native model and effort updates are retained
with the thread; an explicit composer choice takes precedence for the next prompt.

Commands and skills load when a real OMP session starts in a workspace, not during background
status checks. OMP skills then appear in the composer's `$` menu. Your normal OMP configuration
and credentials remain on the server; connecting from another client does not copy credentials
to that client.

OMP has independent releases, so T3 Code does not compare its version with Pi's. The background
health check requires OMP's JSON model catalog; session startup negotiates OMP's native RPC
protocol. If discovery fails, check the executable and authentication on the server, complete
terminal setup, and refresh again. An empty model catalog means OMP has no usable models for
that configuration. Configure API keys as instance environment variables rather than `--api-key`
launch arguments so session and catalog discovery use the same credentials.

## Permissions and sessions

T3 Code loads a permission guard into each OMP session:

- **Supervised** asks before commands, writes, and other non-read tools.
- **Auto-accept edits** additionally allows local filesystem `edit` and `write` calls without
  asking. Writes to tool-backed URLs and hashline-wrapped targets still ask.
- **Full access** skips T3 Code's guard. OMP's explicit restrictions can still require approval.

OMP has no equivalent to T3 Code's automatic reviewer, so **Auto** is not offered. Existing
threads set to Auto use Supervised. These modes are tool policies, not an operating-system sandbox.
OMP questions and approval dialogs appear in the conversation, where you can answer or decline.
Native subagents without an approval UI can use read tools, but guarded operations fail closed
rather than running without your approval.

Threads keep OMP's native session files for continuation. Stopping a turn cancels its current
work; steering sends your follow-up into the running conversation. Native subagent progress and
results use T3 Code's shared subagent view. T3 Code waits for OMP's settled-session signal rather
than treating the end of one agent response as completion while background work remains.

OMP is not offered for T3 Code's application text generation, such as commit-message generation.
Historical OMP usage backfill is not included.

The integration was informed by [Pivot](https://github.com/13kparkin/Pivot)'s OMP work and uses
T3 Code's current provider architecture. The adapted OMP protocol decoder's MIT notice is in
[THIRD_PARTY_NOTICES](../../packages/provider-omp/THIRD_PARTY_NOTICES).
