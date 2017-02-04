from bs4 import BeautifulSoup
import soundcloud

CLIENT_ID = '49009eb8904b11a2a5d2c6bdc162dd32'
client = soundcloud.Client(client_id=CLIENT_ID)

# scraper = pull a user id off a queue and put on a dictionary to another queue

# this doesn't work yet
def get_play_count(user_id):
    # /users/{user_id}/tracks
    print('/users/' + str(user_id) + '/tracks')
    tracks = client.get('/users/' + str(user_id) + '/tracks')
    print(tracks)

def get_info(user_id):
    try:
        path = '/users/' + str(user_id)

        user = client.get(path)
        followings = client.get(path + '/followings').collection
        likes = client.get(path + '/favorites')
        comments = client.get(path + '/comments')
        reposts = client.get('/e1' + path + '/track_reposts')
        #play_count = get_play_count(user_id)
        obj = {
            'user': user,
            'followings': followings,
            'likes': likes,
            'comments': comments,
            'reposts': reposts 
        }
        return obj
    except:
        print('ack it didn\'t work')

def main():
    get_info(16730)

if __name__ == '__main__':
    main()