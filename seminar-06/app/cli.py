import argparse
import sys

from . import rabbit_demo as rabbit
from . import kafka_demo as kafka
from .common import say

STEPS = {
    'rabbit': ('routing', 'redelivery', 'idempotency', 'dlq'),
    'kafka': ('groups', 'replay', 'commit', 'baseline', 'failover', 'min-isr'),
}


def main():
    parser = argparse.ArgumentParser(description='Kafka and RabbitMQ experiments')
    parser.add_argument('area')
    parser.add_argument('step', nargs='?')
    parser.add_argument('args', nargs='*')
    args = parser.parse_args()
    if args.area == 'check':
        conn = rabbit.connect()
        conn.close()
        say('rabbit.ready', vhost='s6')
        kafka.check()
    elif args.area == '_rabbit-worker':
        rabbit.crash_worker(args.step, args.args[0], bool(int(args.args[1])))
    elif args.area == '_kafka-worker':
        kafka.crash_worker(args.step, *args.args)
    elif args.area == 'rabbit':
        actions = {'routing': rabbit.routing, 'redelivery': rabbit.redelivery,
                   'idempotency': lambda: rabbit.redelivery(dedupe=True), 'dlq': rabbit.dlq,
                   'prefetch': rabbit.prefetch}
        if args.step not in actions:
            parser.error('rabbit: routing, redelivery, idempotency, dlq, prefetch')
        actions[args.step]()
    elif args.area == 'kafka':
        actions = {'groups': kafka.groups, 'replay': kafka.replay, 'commit': kafka.commit,
                   'baseline': kafka.baseline, '_leader': kafka.leader, '_after-stop': kafka.after_stop,
                   '_min-isr': kafka.min_isr, '_recovered': kafka.recovered}
        if args.step not in actions:
            parser.error('Use ./lab.sh for failover and min-isr; other steps: groups, replay, commit, baseline')
        actions[args.step]()
    else:
        parser.error('area: check, rabbit, kafka')


if __name__ == '__main__':
    main()
