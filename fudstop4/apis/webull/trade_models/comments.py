import pandas as pd



class WebullComments:
    def __init__(self, data):

        self.subjectType = [i.get('subjectType') for i in data]
        self.rankId = [i.get('rankId') for i in data]
        counter = [i.get('counter') for i in data]
        self.views = [i.get('views') for i in counter]
        self.joiners = [i.get('joiners') for i in counter]
        self.relays = [i.get('relays') for i in counter]
        self.posts = [i.get('posts') for i in counter]
        self.notes = [i.get('notes') for i in counter]
        self.comments = [i.get('comments') for i in counter]
        self.replys = [i.get('replys') for i in counter]
        self.thumbUps = [i.get('thumbUps') for i in counter]
        self.thumbDowns = [i.get('thumbDowns') for i in counter]
        self.thumbs = [i.get('thumbs') for i in counter]
        self.followings = [i.get('followings') for i in counter]
        self.followers = [i.get('followers') for i in counter]
        self.score = [i.get('score') for i in counter]
        self.createTime = [i.get('createTime') for i in data]
        content = [i.get('content') for i in data]
        componentVos = [i.get('componentVos') for i in content]
        componentVos = [item for sublist in componentVos for item in sublist]
        self.uuid = [i.get('uuid') for i in componentVos]
        self.type = [i.get('type') for i in componentVos]
        self.snapshotData = [i.get('snapshotData') for i in content]
        publisher = [i.get('publisher') for i in data]
        self.userId = [i.get('userId') for i in publisher]
        self.userUuid = [i.get('userUuid') for i in publisher]
        self.showType = [i.get('showType') for i in publisher]
        self.liveStatus = [i.get('liveStatus') for i in publisher]
        self.nickName = [i.get('nickName') for i in publisher]
        self.postCount = [i.get('postCount') for i in publisher]
        self.allowPost = [i.get('allowPost') for i in data]
        link = [i.get('link') for i in data]
        labels = [i.get('labels') for i in link]
        labels = [item for sublist in labels for item in sublist]

        self.labelCodes = [i.get('labelCodes') for i in data]
        self.outLink = [i.get('outLink') for i in data]
        self.componentType = [i.get('componentType') for i in data]
        self.childType = [i.get('childType') for i in data]

        self.data_dict = { 
            'subject_type': self.subjectType,
            'rank_id': self.rankId,
            'views': self.views,
            'joiners': self.joiners,
            'relays': self.relays,
            'posts': self.posts,
            'notes': self.notes,
            'comments': self.comments,
            'replys': self.replys,
            'thumb_ups': self.thumbUps,
            'thumb_downs': self.thumbDowns,
            'thumbs': self.thumbs,
            'followings': self.followings,
            'followers': self.followers,
            'score': self.score,
            'uuid': self.uuid,
            'create_time': self.createTime,
            'component_uuid': self.uuid,
            'component_type': self.type,
            'snapshot_data': self.snapshotData,
            'type': self.type,
            'component_type': self.componentType,
            'child_type': self.childType,
            'user_id': self.userId,
            'user_uuid': self.userUuid,
            'nick_name': self.nickName,
            'post_count': self.postCount,

        }


        self.as_dataframe = pd.DataFrame(self.data_dict)

