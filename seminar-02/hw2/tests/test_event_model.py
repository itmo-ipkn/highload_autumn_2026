import uuid

import event
import logic


def test_event_model_fields():
    ev = event.make_event(1, 'user_1_01', 1, 0.02)
    d = ev.to_dict()
    for field in event.REQUIRED_FIELDS:
        assert field in d
    uuid.UUID(d['event_id'])
    assert d['type'] in event.EVENT_TYPES
    assert d['seq'] == 1
    assert isinstance(d['payload'], dict)
    assert d['user_id'] == 'user_1_01'


def test_poison_decision_is_deterministic_and_matches_rate():
    # same input always decides the same way (survives producer restarts)
    assert logic.poison_decision(2, 'user_2_05', 1234, 0.02) == logic.poison_decision(2, 'user_2_05', 1234, 0.02)
    n = 20000
    hits = sum(logic.poison_decision(1, 'user_1_01', s, 0.02) for s in range(1, n + 1))
    assert 0 < hits < n
    assert abs(hits - 0.02 * n) < 0.02 * n  # ~rate, within a generous tolerance
    ev = event.make_event(1, 'user_1_02', 7, 0.02)
    assert bool(ev.payload.get('poison')) == logic.poison_decision(1, 'user_1_02', 7, 0.02)


def test_parse_and_handle():
    ev = event.make_event(1, 'user_1_01', 1, 0.0001)
    obj = event.parse_envelope(ev.to_json())
    assert obj['user_id'] == 'user_1_01'
    event.handle(obj)  # non-poison passes


def test_parse_rejects_missing_fields():
    import json
    bad = json.dumps({'event_id': str(uuid.uuid4()), 'user_id': 'user_1_01',
                      'type': 'purchase', 'seq': 1, 'produced_at': '2026-11-14T00:00:00+00:00'})
    try:
        event.parse_envelope(bad)
    except ValueError as exc:
        assert 'payload' in str(exc)
    else:
        raise AssertionError('missing field must raise')


def test_corrupt_mode_fails_handler():
    ev = event.make_event(1, 'user_1_01', 1, 0.02)
    obj = event.parse_envelope(ev.to_json(corrupt=True))  # envelope still parses
    try:
        event.handle(obj)
    except event.PoisonError:
        pass
    else:
        raise AssertionError('corrupt payload must fail the handler')


def test_poison_flag_fails_handler():
    # find a poison event at this rate and check the handler always fails on it
    for seq in range(1, 5000):
        ev = event.make_event(3, 'user_3_03', seq, 0.02)
        if ev.payload.get('poison'):
            try:
                event.handle(ev.to_dict())
            except event.PoisonError:
                break
    else:
        raise AssertionError('no poison event found at 2% over 5000 seqs')
