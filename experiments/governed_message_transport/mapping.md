# Worker 13 transport handoff mapping

## Accepted upstream pins

The transport work starts from `cogno-us/alvorada@9ad378145d326799e3209136e47e82d66c6f69af` and does not depend on unaccepted PR #2.

Read-only pins inherited from the accepted workbench CI:

- Agent Action Manifest: `cogno-us/cognous-agent-action-manifest@46c950bed37fe3812000895430bc0312d29e37ce`
- Authority Context implementation profile: `cogno-us/constitutional-governance-for-institutions@fb3d97938969a89e149e8ff8db2756091d1233fc`
- Control Plane: `cogno-us/cognous-agent-control-plane@283500652d47a692fb0b99a1172a6d5faffbd9a7`
- Moltbot Safe: `cogno-us/moltbot-safe@1d308faf664c504b6e310db3c7a310153ef7b067`
- Replay Bundle: `cogno-us/cognous-agent-replay-bundle@f63ce914504dd06813c4ccd199b0570dbd8dd427`
- Governance Evidence Pack: `cogno-us/cognous-agent-governance-evidence-pack@f1a76187b72d5b7c9fded12580ba081cb9cba338`
- ODES: `cogno-us/open-decision-evidence-standard@cba83a1c06f718a8afd76178f36e5cc15896347d`

No replacement implementation of those components is included here.

## Interface use

Transport consumes the accepted GAX/IMX functions through `AcceptedGaxRecipientAdapter`:

1. exact governed message arrives;
2. accepted `assess_message` evaluates recipient handling;
3. informational message types stop;
4. eligible PROPOSE/REQUEST messages may enter accepted `run_exchange`;
5. any resulting Control Plane / executor references are returned as producer-owned references;
6. transport persists only its own delivery evidence and explicit links.

Transport never derives authorization from delivery state.

## Unsupported / unresolved

- no production cryptographic transport identity;
- no network protocol, broker, queue service, federation, or service discovery;
- no distributed transaction across transport and execution;
- no independent verifier for destination effects;
- no automatic replay/ODES producer construction in the transport layer;
- no fleet scheduling, task assignment, shared budgets, or authority propagation.

These omissions are boundaries, not silently filled records.
