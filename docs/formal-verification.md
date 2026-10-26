# Formal Verification Options for noq

As of 2026-10-25. Survey of the `formal_verification` branch at commit 07047ffa9.

## Summary

Full formal verification of noq is not a reasonable goal, but a targeted portfolio is: coverage-guided stateful fuzzing and explicit invariants now, bounded model checking of the leaf modules with Kani next, and a TLA+ model of the path and multipath state machine kept honest by validating qlog traces against it. The precedent for verified QUIC is Project Everest's EverQuic, which verified the record layer only. Nobody has verified a full transport with loss recovery and congestion control.

The most important finding from the survey is in the existing test suite. The 25 proptest regression tests carry names like `never_idle`, `infinite_loop`, `challenge_resend_loop`, `probing_loop` and `revalidating_path_forever`. The bugs randomized testing actually finds in noq are not memory or parser bugs. They are protocol liveness bugs in the young multipath and NAT traversal drafts, where two peers get stuck chasing each other. That is exactly the class model checking with liveness properties was built for, and exactly the class deductive code verification is worst at. The recommendations below follow from that.

## What we are working with

The sans-io split is the single biggest asset. `noq-proto` is deterministic and single-threaded, so every property about the protocol can be checked without a runtime, a socket or a clock.

| Component | Size | Verification-relevant trait |
| --- | --- | --- |
| noq-proto | ~58k lines Rust | Sans-io, deterministic, single-threaded. Ideal substrate. |
| connection/mod.rs | ~7.8k lines | One large mutable state machine. The hard part. |
| congestion/bbr3 | ~7k lines | Heuristics with no correctness spec. |
| noq (async layer) | ~9.5k lines | Locks, wakers, tokio. The only concurrency surface. |
| noq-udp | ~3.8k lines | ~90 unsafe FFI sites. Unverifiable trust boundary. |
| unsafe in noq-proto | 8 sites | VarInt unchecked constructor and one BufMut impl. Trivially auditable. |
| asserts and debug_asserts in proto | ~450 | Latent specifications, scattered. |
| Fuzz targets | 4 | Parsers only: packet, params, streamid, streams. |
| proptest regression tests | 25 | Nearly all liveness bugs in multipath and QNT. |
| qlog instrumentation | ~1.3k lines | Ready-made trace source for model validation. |
| Simulated network harness | Pair, Routing, TestOp | Already drives two endpoints with loss, reorder, migration and path ops. |

## What "verify a QUIC implementation" can mean

"Verified" covers seven distinct properties. Each has different tooling, cost and distance from the shipped code, and only some of them hold bugs that matter for noq.

1. **Memory safety and absence of undefined behavior.** Rust gives this nearly for free in proto. The residual is 8 unsafe sites in proto and the FFI in noq-udp. Miri on the test suite covers proto's sites. Nothing formal reaches the syscall boundary.
2. **Panic freedom and arithmetic safety.** A remote panic is a remote denial of service, which is the top security class for a memory-safe QUIC stack. Packet number decoding, varint bounds, stream ID arithmetic, anti-amplification byte counting and range set math are where overflows and index errors live. Bounded model checking on real Rust handles this well.
3. **Functional correctness of pure data structures.** ArrayRangeSet, Assembler, SendBuffer, CidQueue, VarInt and frame encode/decode roundtrip. About 3k lines of pure code with clean specifications. This is the sweet spot for deductive tools.
4. **Protocol conformance and liveness.** Whether the connection, path and stream state machines follow RFC 9000, the multipath draft and the NAT traversal draft, and whether they always make progress. This is where noq's real bugs are. It requires a model, and a model is a second artifact that drifts from the code.
5. **Path validation and anti-amplification.** RFC 9000 section 8 invariants: never send more than three times the received bytes to an unvalidated address, never migrate without validation. Multipath and QNT multiply this surface. These are safety invariants expressible both in a model checker and as runtime invariant checks.
6. **Cryptographic security.** Handshake and record protection live in rustls, ring and aws-lc. aws-lc already ships formally verified components. The pluggable crypto trait is a natural assumption boundary, and verifying it in noq is the wrong use of effort.
7. **Congestion control.** There is no correctness specification for BBR3 or Cubic. Safety bounds like cwnd never zero and no division by zero are provable. Whether it is a good controller is not. Low value per unit of effort.

## The approaches

Twelve approaches are worth weighing. For each: what it checks, what it costs to build and keep, what it buys, and how far the proof sits from the code that ships.

**A. Coverage-guided stateful fuzzing of the existing TestOp harness.** Not formal, but it attacks the right bug class for nearly nothing. `TestOp` already derives `Arbitrary`. Wrapping `run_random_interaction` in a libfuzzer target moves from proptest's blind sampling to coverage guidance. The four current fuzz targets only cover parsers. Maintenance: near zero, it is the harness already maintained. Gap: none, it runs the real code.

**B. Extract invariants into an explicit checker.** Pull the ~450 scattered asserts into a `check_invariants` method on `Connection` and call it after every step in simulated tests. This is the first step of every approach below, because it forces a written definition of "correct". It also serves as the oracle for A. Maintenance: low, invariants change when the design does.

**C. Bounded model checking on real Rust with Kani.** Harnesses for VarInt, ArrayRangeSet, Assembler, SendBuffer, CidQueue, packet number decode, frame roundtrip and the amplification arithmetic. Kani runs on the shipped code, in CI, as something that reads like a unit test with symbolic inputs. Weakness: loop unwinding bounds and heap-heavy types like growing Vec and FxHashMap. It will not scale to connection/mod.rs. Maintenance: low to medium, harnesses break when signatures change, like tests. Gap: zero for what it covers, but the result is "no bug up to size N", not a proof.

**D. Refinement types with Flux.** Lightweight annotations on real Rust for bounds and overflow. Lower proof burden than C and covers panic freedom broadly. Still maturing, with nontrivial build tooling. Medium maintenance.

**E. Deductive verification of leaf modules with Creusot, Verus or Aeneas.** Full functional proofs for the pure data structures. Creusot annotates real Rust and exports to Why3. Verus requires rewriting in its own dialect, meaning a second copy of the code, which is a maintenance trap. Aeneas translates Rust to Lean and suits pure code. Feasible as a one-time project for ~3k lines of leaf code. Extending to the connection core is research, not engineering. Gap: Creusot and Kani verify the code that ships. Verus verifies a sibling.

**F. Hax extraction to F\* or ProVerif.** Cryspen used this for libcrux and the Bertie TLS stack. It needs a restricted Rust subset, and proto uses Bytes, Box, trait objects and hash maps throughout. Realistic only for frame and packet codecs. High maintenance.

**G. A TLA+ or Quint model of the path and multipath state machine.** Model path lifecycle, PATH_CHALLENGE and PATH_RESPONSE, path abandon, QNT rounds, address validation and connection ID management. Check safety properties like amplification limits and liveness properties like "every opened path is eventually validated or abandoned". This directly targets the regression list's bug class, and it finds bugs in the drafts themselves while they are still drafts. Cost: a few weeks for a useful model. Maintenance: the model drifts from the code unless paired with H. Gap: large on its own.

**H. Trace validation linking the model to the implementation.** noq already has ~1.3k lines of qlog instrumentation. Emit qlog from the simulated network tests, map events to model actions, and check with TLC or Apalache that every observed trace is a behavior of the model. MongoDB and others use this pattern. It is the one technique that closes the model-to-code gap at acceptable cost, and it keeps G honest because a code change that breaks the model fails CI. Maintenance: medium, the event mapping must be kept current. Value: turns G from documentation into a checked spec.

**I. Spec-derived adversarial conformance testing, Ivy style.** McMillan and Zuck wrote a formal QUIC specification in Ivy and generated adversarial peers from it, finding bugs in several implementations. Adapting that to multipath and QNT is research work. Highest value for interop correctness, highest cost. Sensible only as a funded collaboration.

**J. Symbolic protocol analysis in Tamarin or ProVerif.** Covers cryptographic security. Several academic analyses of QUIC's handshake exist. Given crypto is delegated to rustls, this verifies someone else's code against an abstract model. Low marginal value.

**K. Concurrency model checking of the noq async crate with Loom.** Covers lock and waker races in the ~9.5k line async layer. Modest cost, targeted. Not about QUIC correctness, but about the one place proto's determinism does not help.

**L. Differential testing against quinn and other stacks.** Not formal. Cheap, and it catches conformance drift in the standardized core that the extensions might disturb. The interop runner already exists.

## Comparison

| Approach | Formal? | Build cost | Maintenance | Value for noq's bug class | Gap to shipped code |
| --- | --- | --- | --- | --- | --- |
| A. Stateful fuzzing of TestOp | No | Days | Near zero | High, finds liveness bugs | None |
| B. Explicit invariant checker | No | Days to a week | Low | High, prerequisite for all others | None |
| C. Kani bounded model checking | Yes, bounded | Weeks | Low to medium | Medium, panic freedom on remote paths | None, but bounded |
| D. Flux refinement types | Yes | Weeks | Medium | Medium, overflow and bounds | None, tooling immature |
| E. Creusot / Verus / Aeneas on leaf modules | Yes | Months | Medium (Creusot) to high (Verus) | Medium, data structures rarely buggy | None (Creusot) to a sibling codebase (Verus) |
| F. Hax extraction | Yes | Months | High | Low, codecs only | Extracted model, subset of Rust |
| G. TLA+ / Quint model | Yes | Weeks | Medium, drifts without H | High, matches the regression list | Large alone |
| H. qlog trace validation | Yes | Weeks on top of G | Medium | High, makes G a checked spec | Small, every CI run checks real traces |
| I. Ivy-style conformance tester | Yes | Quarters | High | Very high for interop | Small, tests the real binary |
| J. Tamarin / ProVerif handshake | Yes | Months | Low after build | Low, crypto is delegated | Abstract model |
| K. Loom on async layer | Yes, exhaustive for small models | Weeks | Low | Medium, races in noq crate | Small |
| L. Differential interop testing | No | Days | Low | Medium, core conformance | None |

## Recommended portfolio

Match tools to bug classes rather than aim for a verified stack. Four tiers, in order.

1. **Now, cheap, not formal: A and B.** Fuzz the TestOp harness with coverage guidance and make the invariants explicit. About a week of work. This would likely surface more of the same bug class the 25 regressions came from, and B produces the written invariants every later tier needs.
2. **Formal, low maintenance: C.** Kani harnesses for VarInt, ArrayRangeSet, Assembler, SendBuffer, CidQueue, packet number decoding and the amplification arithmetic, run nightly alongside the existing `proptests-long` job. This buys real panic-freedom guarantees on the remote-reachable parsing and bookkeeping paths. Also cover the 8 unsafe sites in proto.
3. **Formal, highest value: G plus H together.** Model the path and multipath state machine in TLA+ or Quint with liveness properties, and validate qlog traces from the simulated network tests against it in CI. Without H, G rots. With H, noq gets a checked specification of its extensions while the drafts are still changing. This is also the strongest contribution the project could make back to the IETF multipath and NAT traversal work.
4. **Skip unless funded as research: E on the connection core, F, I and J.** Treat rustls, aws-lc, the OS socket layer, tokio and the compiler as the trusted base, and write that down.

K and L are cheap enough to pick up opportunistically. L in particular is nearly free given the interop runner already exists.

## What remains unverified

Even with tiers 1 through 3 done, the guarantees have edges, and they should be stated as plainly as the guarantees themselves.

- **Kani is bounded, not total.** Each harness proves absence of bugs up to an input size and loop depth. Larger inputs are covered by fuzzing, not proof.
- **The TLA+ model abstracts.** Timers, congestion control and byte-level framing are not in the model. A trace validator checks that the code's observable behavior matches the model, not that the model captures everything the code does.
- **noq-udp and the runtime are untouched.** The ~90 unsafe FFI sites, the sendmsg and recvmsg paths, GSO and GRO handling, and tokio's scheduler are outside every tool here. They are the trusted base along with rustls, aws-lc and rustc.
- **Congestion control stays heuristic.** BBR3 and Cubic get safety bounds at most. Their behavior under real networks is a matter of measurement, not proof.
- **The specifications are moving.** The multipath and NAT traversal drafts change. A model that tracks them is a maintenance commitment for as long as the drafts are unstable, and H makes that commitment visible in CI rather than silent.

That is still a materially stronger position than any shipping QUIC stack today, and it stays maintainable by a small team because every artifact runs in CI against the real code.

## References

Links were written from memory and not fetched while drafting. Verify before citing onward.

**Specifications**

- [RFC 9000, QUIC transport](https://www.rfc-editor.org/rfc/rfc9000.html), section 8 for address validation and anti-amplification
- [RFC 9002, loss detection and congestion control](https://www.rfc-editor.org/rfc/rfc9002.html)
- [QUIC Multipath draft](https://datatracker.ietf.org/doc/draft-ietf-quic-multipath/)
- [QUIC NAT traversal draft](https://datatracker.ietf.org/doc/draft-seemann-quic-nat-traversal/)
- [QUIC Address Discovery draft](https://datatracker.ietf.org/doc/draft-ietf-quic-address-discovery/)

**Rust verification tools**

- [Kani](https://github.com/model-checking/kani), bounded model checker for Rust, CBMC backend
- [Creusot](https://github.com/creusot-rs/creusot), deductive verification of annotated Rust via Why3
- [Verus](https://github.com/verus-lang/verus), SMT-based verification in a Rust dialect
- [Aeneas](https://github.com/AeneasVerif/aeneas), Rust to Lean and F\* translation
- [Flux](https://github.com/flux-rs/flux), refinement types for Rust
- [Hax](https://github.com/cryspen/hax), Rust to F\*, Coq and ProVerif extraction
- [Loom](https://github.com/tokio-rs/loom), concurrency permutation testing
- [Miri](https://github.com/rust-lang/miri), undefined behavior detection

**Protocol modeling**

- [TLA+](https://lamport.azurewebsites.net/tla/tla.html) and [Apalache](https://apalache-mc.org/)
- [Quint](https://github.com/informalsystems/quint), a typed specification language that compiles to TLA+
- McMillan and Zuck, "Formal specification and testing of QUIC", SIGCOMM 2019, the Ivy QUIC specification and adversarial tester
- Davis et al., "eXtreme Modelling in Practice", VLDB 2020, MongoDB's trace validation of TLA+ specs against the implementation

**Prior verified QUIC and TLS work**

- Delignat-Lavaud et al., "A Security Model and Fully Verified Implementation for the IETF QUIC Record Layer", IEEE S&P 2021, the EverQuic record layer from Project Everest
- [Bertie](https://github.com/cryspen/bertie), a TLS 1.3 implementation verified through Hax
- [AWS-LC formal verification](https://github.com/awslabs/aws-lc-verification), the verified components under aws-lc-rs
