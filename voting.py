import simplejson as json
import random
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, SerializingProducer


from main import delivery_report

conf ={
    'bootstrap.server':'localhost:9092'
}

consumer = Consumer(conf | {
    'group.id':'voting-group',
    'auto.offset.reset' : 'earliest',
    'enable.auto.commit':False
})

producer =SerializingProducer(conf)


if __name__ =="__main__":
    conn = psycopg2.connect(
        dbname="election",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5433"
    )
    cur = conn.cursor()

    candidates_query = cur.execute(
        """
        SELECT row_to_json(col)
        from (
        SELECT * FROM candidates
        ) col
        """

    )

    candidates= [candidate[0] for candidate in cur.fetchall()]

    if len(candidates) == 0:
        raise Exception("No candidate found")
    else:
        print(candidates)


    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote':1
                }

                try:
                    print('User is {} voting for candidate: {}'.format(vote['voter_id'],vote['candidate_id']))
                    cur.execute("""
                    INSERT INTO votes (voter_id, candidate_id, voting_time)
                    VALUES (%s, %s, %s)
                    """, (vote['voter_id'],vote['candidate_id'],vote['voting_time']))
                    conn.commit()

                    producer.produce(
                        'votes_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )

                    producer.poll(0)



                except Exception as e:
                    print(e)
    except Exception as e:
        print(e)