from collections import Counter
import sqlite3
import sys
import networkx as nx
import operator

USAGE = 'Usage: python3 influence.py [--db <db_name>]'

def main():
    db = 'sc_graph.sqlite3'
    if len(sys.argv) == 3:
        db = sys.argv[2]
    elif len(sys.argv) > 3 or len(sys.argv) == 2:
        print(USAGE)

    print('Building graph...')
    G, user_ids_to_urls = build_graph_from_db(db)
    print('Finished building graph. Ranking...')
    ranking = nx.pagerank(G)
    print('Done ranking.')

    sorted_ranking = sorted(ranking.items(), key=operator.itemgetter(1))

    for user_id, rank in sorted_ranking[-100:]:
        user = str(user_id)
        print("{}: {}".format(user_ids_to_urls[user] if user in user_ids_to_urls else user, rank))


def build_graph_from_db(db):
    conn = sqlite3.connect(db)
    G = nx.DiGraph()

    user_ids_to_urls = {str(user[0]): str(user[1]) for user in conn.execute('SELECT id, url FROM users')}

    for user_id, _ in user_ids_to_urls.items():
        following = [following_id[0] for following_id in conn.execute('SELECT following_id FROM following WHERE id = ?', (user_id,))]
        commented_on = [target_id[0] for target_id in conn.execute('SELECT target_id FROM comments WHERE id = ?', (user_id,))]
        likes = [target_id[0] for target_id in conn.execute('SELECT target_id FROM likes WHERE id = ?', (user_id,))]
        reposted = [target_id[0] for target_id in conn.execute('SELECT target_id FROM reposts WHERE id = ?', (user_id,))]

        combined = Counter(following + commented_on + likes + reposted)
        for target, weight in combined.items():
            G.add_edge(user_id, target, weight=weight)

    return G, user_ids_to_urls

if __name__ == '__main__':
    main()
