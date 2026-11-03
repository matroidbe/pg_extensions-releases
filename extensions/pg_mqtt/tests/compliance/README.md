# MQTT compliance: the Paho interoperability suites

`run_paho.py` runs the Eclipse Paho interoperability tests
(`eclipse-paho/paho.mqtt.testing`: `interoperability/client_test5.py` for MQTT 5,
`client_test.py` for MQTT 3.1.1) against a running pg_mqtt, one test per
process with a time limit. It then checks the result against `profile.toml`.
A test counts as passed only when unittest reports it ran and passed.

MQTT has no official certification: OASIS publishes the specification with
numbered normative statements and no test suite. The Paho suites are the
interoperability check brokers are usually run against. A broker that passes them
is not proven compliant statement by statement (Eclipse IoT-Testware's TTCN-3
suites come closer), but a failing test is a real gap.

## The profile

`profile.toml` gives every test of each suite a status:

- **required**: the feature is in pg_mqtt's supported profile and the test
  must pass.
- **exempt**: the feature is not supported (yet), with the reason. The broker
  must still refuse it the way the specification says. For example, CONNACK
  announces "shared subscriptions not available" and a `$share/` SUBSCRIBE
  gets reason 0x9E.

The profile is the minimum the UNS+ MVP needs: Sparkplug B and plain JSON
devices, gateways speaking MQTT 3.1.1, and the Eidos protocol plugin as a
client. The `exempt` entries are the backlog for full MQTT.

The run fails when a required test fails, when a suite has a test the profile
does not list, or when the profile lists one the suite no longer has. An exempt
test that starts passing is reported, so its exemption can be dropped.

## Running it

```bash
git clone https://github.com/eclipse-paho/paho.mqtt.testing.git
git -C paho.mqtt.testing checkout fdab9d2b1ce125385135e5d9f282a0ad315268cc
python3 run_paho.py --suite paho.mqtt.testing/interoperability --host localhost --port <pg_mqtt.port>
```

`--only-suite mqtt311` (or `mqtt5`) runs one suite. Run it against a throwaway
database, because the suites publish and clear retained messages.

Not covered here:

- The Eclipse Sparkplug TCK, which checks the Sparkplug broker profile on top
  of plain MQTT.
