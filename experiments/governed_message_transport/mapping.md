# Worker 13 transport handoff mapping

## Accepted upstream pins

The transport work starts from `cogno-us/alvorada@9ad378145d326799e3209136e47e82d66c6f69af` and does not depend on unaccepted PR #2.

Read-only pins inherited from the accepted workbench CI:

- Agent Action Manifest: `cogno-us/cognous-agent-action-manifest@46c950bed37fe3812000895430bc0312d29e37ce`
- Authority Context implementation profile: `cogno-us/constitutional-governance-for-institutions@fb3d97938969a89e149e8ff8db2756091d1233fc`
- Control Plane: `cogno-us/cognous-agent-control-plane@283500652d47a692fb0b99a1172a6d5faffbd9a7`
- Moltbot Safe: `cogno-us/moltbot-safe@6b0ba1185bcd390f71df947dda349415e4105f5f`
- Replay Bundle: `cogno-us/cognous-agent-replay-bundle@f12648313cedc2cf06145d397fa56cdea18cc800`
- Governance Evidence Pack: `cogno-us/cognous-agent-governance-evidence-pack@c699c1fb7c4f8057631c4e5909d11a721c2c958d`
- ODES: `cogno-us/open-decision-evidence-standard@b3a2f1e72df88cd24d93d1b7d69963f43139e749`

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
