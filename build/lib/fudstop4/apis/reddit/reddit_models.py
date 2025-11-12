import pandas as pd


class RedditPost:
    def __init__(self, post):
        self.comment_limit = post.comment_limit
        self.comment_sort = post.comment_sort
        self._reddit = post._reddit
        self.approved_at_utc = post.approved_at_utc
        self.subreddit = post.subreddit
        self.selftext = post.selftext
        self.author_fullname = post.author_fullname
        self.saved = post.saved
        self.mod_reason_title = post.mod_reason_title
        self.gilded = post.gilded
        self.clicked = post.clicked
        self.title = post.title
        self.link_flair_richtext = post.link_flair_richtext
        self.subreddit_name_prefixed = post.subreddit_name_prefixed
        self.hidden = post.hidden
        self.pwls = post.pwls
        self.link_flair_css_class = post.link_flair_css_class
        self.downs = post.downs
        self.thumbnail_height = post.thumbnail_height
        self.top_awarded_type = post.top_awarded_type
        self.hide_score = post.hide_score
        self.name = post.name
        self.quarantine = post.quarantine
        self.link_flair_text_color = post.link_flair_text_color
        self.upvote_ratio = post.upvote_ratio
        self.author_flair_background_color = post.author_flair_background_color
        self.subreddit_type = post.subreddit_type
        self.ups = post.ups
        self.total_awards_received = post.total_awards_received
        self.media_embed = post.media_embed
        self.thumbnail_width = post.thumbnail_width
        self.author_flair_template_id = post.author_flair_template_id
        self.is_original_content = post.is_original_content
        self.user_reports = post.user_reports
        self.secure_media = post.secure_media
        self.is_reddit_media_domain = post.is_reddit_media_domain
        self.is_meta = post.is_meta
        self.category = post.category
        self.secure_media_embed = post.secure_media_embed
        self.link_flair_text = post.link_flair_text
        self.can_mod_post = post.can_mod_post
        self.score = post.score
        self.approved_by = post.approved_by
        self.is_created_from_ads_ui = post.is_created_from_ads_ui
        self.author_premium = post.author_premium
        self.thumbnail = post.thumbnail
        self.edited = post.edited
        self.author_flair_css_class = post.author_flair_css_class
        self.author_flair_richtext = post.author_flair_richtext
        self.gildings = post.gildings
        self.content_categories = post.content_categories
        self.is_self = post.is_self
        self.mod_note = post.mod_note
        self.created = post.created
        self.link_flair_type = post.link_flair_type
        self.wls = post.wls
        self.removed_by_category = post.removed_by_category
        self.banned_by = post.banned_by
        self.author_flair_type = post.author_flair_type
        self.domain = post.domain
        self.allow_live_comments = post.allow_live_comments
        self.selftext_html = post.selftext_html
        self.likes = post.likes
        self.suggested_sort = post.suggested_sort
        self.banned_at_utc = post.banned_at_utc
        self.view_count = post.view_count
        self.archived = post.archived
        self.no_follow = post.no_follow
        self.is_crosspostable = post.is_crosspostable
        self.pinned = post.pinned
        self.over_18 = post.over_18
        self.all_awardings = post.all_awardings
        self.awarders = post.awarders
        self.media_only = post.media_only
        self.can_gild = post.can_gild
        self.spoiler = post.spoiler
        self.locked = post.locked
        self.author_flair_text = post.author_flair_text
        self.treatment_tags = post.treatment_tags
        self.visited = post.visited
        self.removed_by = post.removed_by
        self.num_reports = post.num_reports
        self.distinguished = post.distinguished
        self.subreddit_id = post.subreddit_id
        self.author_is_blocked = post.author_is_blocked
        self.mod_reason_by = post.mod_reason_by
        self.removal_reason = post.removal_reason
        self.link_flair_background_color = post.link_flair_background_color
        self.id = post.id
        self.is_robot_indexable = post.is_robot_indexable
        self.report_reasons = post.report_reasons
        self.author = post.author
        self.discussion_type = post.discussion_type
        self.num_comments = post.num_comments
        self.send_replies = post.send_replies
        self.contest_mode = post.contest_mode
        self.mod_reports = post.mod_reports
        self.author_patreon_flair = post.author_patreon_flair
        self.author_flair_text_color = post.author_flair_text_color
        self.permalink = post.permalink
        self.stickied = post.stickied
        self.url = post.url
        self.subreddit_subscribers = post.subreddit_subscribers
        self.created_utc = post.created_utc
        self.num_crossposts = post.num_crossposts
        self.media = post.media
        self.is_video = post.is_video
        self._fetched = post._fetched
        self._additional_fetch_params = post._additional_fetch_params
        self._comments_by_id = post._comments_by_id

    def as_dataframe(self):
        """
        Convert all post attributes to a pandas DataFrame.
        :return: pandas.DataFrame
        """
        attributes = {attr: getattr(self, attr) for attr in self.__dict__.keys()}
        return pd.DataFrame([attributes])