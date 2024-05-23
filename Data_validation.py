import json
import boto3
from datetime import datetime
import pandas as pd

           
 
s3 = boto3.client('s3')
ses = boto3.client('ses')

lambda_client = boto3.client('lambda')

lambda_for_missing_tags = 'arn:aws:lambda:eu-north-1:933792340412:function:moving_sprinklr_daily_weekely_and_tag_on_failed'
lambda_for_processing_tags = 'arn:aws:lambda:eu-north-1:933792340412:function:moving_sprinklr_daily_and_weekely_tagpull_file'
 
    
source_bucket='amazon-global-gsmc-sprinklr'
source_weekly_folder='RAWDATA/Fluency-Weekly/'
source_daily_folder='RAWDATA/Fluency-Monthly/'
source_tag_folder='RAWDATA/Tag-Pull/'
destination_bucket='wikitablescrapexample'
destination_weekely_folder='amazon_sprinklr_pull/Fluency-Weekly/'
destination_daily_folder='amazon_sprinklr_pull/fluency/'
destination_tag_folder='amazon_sprinklr_pull/Tag-Pull/'
follower_data_destination='amazon_sprinklr_pull/follower/'
follower_data_filename='Follower_Data_7_FluencyWeekly.json'
    
expected_file_structure = {
        
"Facebook_Pull_1_FluencyMonthly.json": {
    "Number of Columns": 42,
    "Column Names": [
      "PUBLISHEDTIME",
      "POST_COMMENTS",
      "ACCOUNT_TYPE",
      "FACEBOOK_VIDEO_LENGTH",
      "POST_LIKES_AND_REACTIONS",
      "FACEBOOK_POST_CONSUMPTIONS_VIDEO_PLAY",
      "STATUS",
      "ACCOUNT",
      "CAMPAIGN_NAME",
      "MEDIA_TYPE",
      "OUTBOUND_POST",
      "FACEBOOK_VIDEO_ORGANIC_VIEWS__VIEWED_FOR_3_SECONDS_OR_MORE",
      "FACEBOOK_POST_PAID_IMPRESSIONS",
      "SCHEDULEDTIME",
      "NET_SENTIMENT",
      "CAMPAIGN",
      "FACEBOOK_VIDEO_AVERAGE_TIME_VIEWED",
      "PERMALINK",
      "MESSAGE_TYPE",
      "AUTHOR_NAME",
      "FACEBOOK_POST_ORGANIC_REACH",
      "MEDIA_SOURCE",
      "FACEBOOK_POST_CONSUMPTIONS_LINK_CLICK",
      "FACEBOOK_VIDEO_ORGANIC_VIEWS__VIEWED_95__TO_VIDEO_LENGTH",
      "FACEBOOK_POST_ENGAGED_USERS",
      "DATE",
      "FACEBOOK_POST_CONSUMPTIONS_PHOTO_VIEW",
      "IS_DARK_POST",
      "GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE",
      "FACEBOOK_POST_ORGANIC_IMPRESSIONS",
      "POST_SHARES",
      "TAGS",
      "FACEBOOK_POST_CONSUMPTIONS_OTHER_CLICK",
      "GSMC__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE", 
      "GSMC__IS_IT_XGC___OUTBOUND_MESSAGE", 
      "EMEA_-_PR__ORGANIC__TEAM__OUTBOUND_MESSAGE",  
      "EMEA__-PR__CM__OVERARCHING_TEAM__OUTBOUND_MESSAGE", 
      "EMEA_-PR__ORGANIC__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE",
      "EMEA_-_PR__CM__GLOBAL_SOCIAL_GOAL__OUTBOUND_MESSAGE" ,
      "GSMC__IF_PRODUCTS___SERVICES__WHICH_ONE___OUTBOUND_MESSAGE",
      "GSMC__CONTENT_CATEGORY_TYPE__OUTBOUND_MESSAGE", 
      "GSMC__WHO_IS_THE_SOURCE_OF_THE_XGC___OUTBOUND_MESSAGE"
    ]  
  },   
"Instagram_3_FluencyMonthly.json": {
    "Number of Columns": 41,
    "Column Names": [
      "PUBLISHEDTIME",
      "ACCOUNT_TYPE",
      "C_63DAC29E6DF27A45C681FF10",
      "INSTAGRAM_POST_LIKES__SUM",
      "POST_LIKES_AND_REACTIONS__SUM",
      "STATUS",
      "ACCOUNT",
      "62696510869F0F319C9BB628",
      "INSTAGRAM_POST_COMMENTS__SUM",
      "CAMPAIGN_NAME",
      "MEDIA_TYPE",
      "OUTBOUND_POST",
      "6269650F869F0F319C9BB4DF",
      "SCHEDULEDTIME",
      "NET_SENTIMENT_IN_",
      "CAMPAIGN",
      "C_62D5EEB82AAE8171DF12E8E3",
      "INSTAGRAM_BUSINESS_POST_SAVED__SUM",
      "PERMALINK",
      "INSTAGRAM_BUSINESS_POST_ENGAGEMENT__SUM",
      "MESSAGE_TYPE",
      "AUTHOR_NAME",
      "EB_-_SPECIFIC_TEAM_AMAZON_MARKETING___OUTBOUND_MESSAGE",
      "62696512869F0F319C9BB7FD",
      "INSTAGRAM_BUSINESS_POST_REACH__SUM",
      "TOTAL_IMPRESSIONS__SUM",
      "C_63DAC6AC6DF27A45C687B642",
      "C_63E1769D6DF27A45C612A80C",
      "MEDIA_SOURCE",
      "INSTAGRAM_VIDEO_VIEWS__SUM",
      "FACEBOOK_VIDEO_VIEWS__VIEWED_FOR_3_SECONDS_OR_MORE___SUM",
      "C_63DAC8FF6DF27A45C68AAF7C",
      "DATE",
      "INSTAGRAM_BUSINESS_POST_IMPRESSIONS__SUM",
      "IS_DARK_POST",
      "GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE",
      "INSTAGRAM_BUSINESS_POST_INITIAL_REEL_PLAYS__SUM",
      "POST_SHARES__SUM",
      "TAGS",
      "INSTAGRAM_BUSINESS_POST_TOTAL_REEL_PLAYS__SUM",
      "INSTAGRAM_BUSINESS_POST_SHARES__SUM"
    ]
  },  
 
  "Instagram_Story_5_FluencyMonthly.json": {
    "Number of Columns": 17,
    "Column Names": [
      "PUBLISHEDTIME",
      "C_62D5EEB82AAE8171DF12E8E3",
      "ACCOUNT_TYPE",
      "PERMALINK",
      "EVENT_CAMPAIGN_AMAZON_MARKETING___OUTBOUND_MESSAGE",
      "INSTAGRAM_BUSINESS_POST_STORY_REPLIES__SUM",
      "AUTHOR_NAME",
      "POST_LIKES_AND_REACTIONS__SUM",
      "STATUS",
      "ACCOUNT",
      "INSTAGRAM_BUSINESS_POST_IMPRESSIONS__SUM",
      "INSTAGRAM_STORY_REACH__SUM",
      "CAMPAIGN_NAME",
      "OUTBOUND_POST",
      "SCHEDULEDTIME",
      "TAGS",
      "INSTAGRAM_BUSINESS_POST_STORY_TAPS_FORWARD__DEPRECATED___SUM"
    ]
  },   
  "LinkedIn_6_FluencyMonthly.json": {
    "Number of Columns": 32,
    "Column Names": [
      "PUBLISHEDTIME",
      "ACCOUNT_TYPE",
      "C_63DAC29E6DF27A45C681FF10",
      "LINKEDIN_VIDEO_VIEWS__SUM",
      "POST_REACH__SUM",
      "AWS_MKTG__LINKEDIN_CLICK-THROUGH_RATE_IN_",
      "LINKEDIN_COMPANY_POST_COMMENTS__SUM",
      "LINKEDIN_COMPANY_POST_LIKES_AND_REACTIONS__SUM",
      "STATUS",
      "ACCOUNT",
      "CAMPAIGN_NAME",
      "MEDIA_TYPE",
      "LINKEDIN_COMPANY_POST_SHARES__SUM",
      "OUTBOUND_POST",
      "SCHEDULEDTIME",
      "LINKEDIN_COMPANY_POST_IMPRESSIONS__SUM",
      "NET_SENTIMENT_IN_",
      "C_62D5EEB82AAE8171DF12E8E3",
      "LINKEDIN_COMPANY_POST_COMMENTS_AND_REPLIES__SUM",
      "PERMALINK",
      "MESSAGE_TYPE",
      "AUTHOR_NAME",
      "C_63DAC6AC6DF27A45C687B642",
      "C_63E1769D6DF27A45C612A80C",
      "MEDIA_SOURCE",
      "C_63DAC8FF6DF27A45C68AAF7C",
      "LINKEDIN_COMPANY_POST_CLICKS__SUM",
      "LINKEDIN_COMPANY_POST_ENGAGEMENT_RATE__SUM",
      "LINKEDIN_VIDEO_TOTAL_WATCH_TIME__SUM",
      "LINKEDIN_COMPANY_POST_ORGANIC_ENGAGEMENTS__SUM",
      "TAGS",
      "IS_DARK_POST"
    ]
  },
  "Twitter_2_FluencyMonthly.json": {
    "Number of Columns": 38,
    "Column Names": [
      "PUBLISHEDTIME",
      "POST_COMMENTS__SUM",
      "ACCOUNT_TYPE",
      "C_63DAC29E6DF27A45C681FF10",
      "X_MEDIA_VIEWS__SUM",
      "X_TOTAL_ENGAGEMENTS__SUM",
      "POST_LIKES_AND_REACTIONS__SUM",
      "STATUS",
      "ACCOUNT",
      "62696510869F0F319C9BB628",
      "TWITTER_ENGAGEMENTS__SUM",
      "CAMPAIGN_NAME",
      "MEDIA_TYPE",
      "X_VIDEO_VIEWED_100___SUM",
      "OUTBOUND_POST",
      "6269650F869F0F319C9BB4DF",
      "FACEBOOK_POST_PAID_IMPRESSIONS__SUM",
      "SCHEDULEDTIME",
      "NET_SENTIMENT_IN_",
      "CAMPAIGN",
      "C_62D5EEB82AAE8171DF12E8E3",
      "PERMALINK",
      "MESSAGE_TYPE",
      "AUTHOR_NAME",
      "EB_-_SPECIFIC_TEAM_AMAZON_MARKETING___OUTBOUND_MESSAGE",
      "62696512869F0F319C9BB7FD",
      "C_63DAC6AC6DF27A45C687B642",
      "C_63E1769D6DF27A45C612A80C",
      "MEDIA_SOURCE",
      "C_63DAC8FF6DF27A45C68AAF7C",
      "DATE",
      "IS_DARK_POST",
      "GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE",
      "POST_SHARES__SUM",
      "X_VIDEO_VIEWS__SUM",
      "X_IMPRESSIONS__SUM",
      "X_URL_CLICKS__SUM",
      "TAGS"
    ]
  },
  "YouTube_10_FluencyMonthly.json": {
    "Number of Columns": 19,
    "Column Names": [
      "PUBLISHEDTIME",
      "YOUTUBE_VIDEO_AVERAGE_VIEW_DURATION__SUM",
      "YOUTUBE_PAID_VIEWS__SUM",
      "ACCOUNT_TYPE",
      "PERMALINK",
      "YOUTUBE_VIDEO_VIEWS__SUM",
      "YOUTUBE_VIDEO_COMMENTS__SUM",
      "AUTHOR_NAME",
      "DATE",
      "STATUS",
      "ACCOUNT",
      "CAMPAIGN_NAME",
      "OUTBOUND_POST",
      "SCHEDULEDTIME",
      "YOUTUBE_VIDEO_LIKES__SUM",
      "YOUTUBE_VIDEO_SHARES__SUM",
      "YOUTUBE_VIEW_GREATER_THAT_95___SUM",
      "NET_SENTIMENT_IN_",
      "TAGS"
    ]
  },
  "User_Group_Lookup_9_FluencyMonthly.json": {
    "Number of Columns": 13,
    "Column Names": [
      "PUBLISHEDTIME",
      "ACCOUNT_TYPE",
      "PERMALINK",
      "USER_GROUP",
      "AUTHOR_NAME",
      "TOTAL_ENGAGEMENTS__SUM",
      "STATUS",
      "ACCOUNT",
      "CAMPAIGN_NAME",
      "OUTBOUND_POST",
      "SCHEDULEDTIME",
      "TAGS",
      "ACCOUNT_GROUP"
    ]
  }, 
  "Paid_Data_11_FluencyWeekly.json": {
    "Number of Columns": 24,
    "Column Names": [
      "TWITTER_POSTS_IMPRESSIONS__SUM",
      "FACEBOOK_AVG__DURATION_OF_VIDEO_PLAYED__SUM",
      "FACEBOOK_COST_PER_DAILY_ESTIMATED_AD_RECALL_LIFT__PEOPLE__IN_DEFAULT__SUM",
      "COST_PER_1_000_IMPRESSIONS__CPM__IN_DEFAULT__SUM",
      "IMPRESSIONS__SUM",
      "LINK_CLICKS__FB___LI___TW___SUM",
      "TWITTER_POST_SPENT__USD__IN_USD__SUM",
      "ACM_GLOBAL_SHARES__SUM",
      "ANKUR_VIDEO_THRU_PLAY_OR_15_SEC_IN_DEFAULT__SUM",
      "DAILY_ESTIMATED_AD_RECALL_LIFT__PEOPLE___SUM",
      "AD_ACCOUNT",
      "TOTAL_RESULTS_FOR_OBJECTIVE__SUM",
      "AD_VARIANT_NAME",
      "ACM_GLOBAL_COMMENTS__SUM",
      "CHANNEL",
      "ACM_GLOBAL_LIKES__SUM",
      "AD_POST_PERMALINK",
      "AD_OBJECTIVE",
      "CLICKS__SUM",
      "ACM_GLOBAL_TOTAL_ENGAGEMENTS__SUM",
      "ACM_GLOBAL_VIDEO_VIEWS__SUM",
      "SPENT__USD__IN_USD__SUM",
      "ACM_GLOBAL_CONVERSIONS__SUM",
      "RESULTS_PER_AMOUNT_SPENT__SUM"
    ]
  },
  "Follower_Data_7_FluencyWeekly.json": {
    "Number of Columns": 8,
    "Column Names": [
      "DATE",
      "ACCOUNT",
      "SOCIAL_NETWORK",
      "INSTAGRAM_FOLLOWERS_COUNT_BY_COUNTRY__SUM",
      "FACEBOOK_PAGE_TOTAL_LIKES__SUM",
      "FACEBOOK_PAGE_FOLLOWERS_COUNT__SUM",
      "FOLLOWERS__SUM",
      "POST_LIKES_AND_REACTIONS__SUM"
    ]
  },       
  "Paid_Tags_15_FluencyWeekly.json": {
    "Number of Columns": 18,
    "Column Names": [
      "C_62D5EEB82AAE8171DF12E8E3" ,
      "SPENT_IN_DEFAULT__SUM",
      "C_63DAC29E6DF27A45C681FF10",
      "EVENT_CAMPAIGN_AMAZON_MARKETING___OUTBOUND_MESSAGE",
      "GCCI_SOME__ADS__AUDIENCE__AD_SET",
      "GCCI_SOME__ADS__CAMPAIGN_DESCRIPTION__AD_SET",
      "C_63DAC6AC6DF27A45C687B642",
      "C_63DAC8FF6DF27A45C68AAF7C",
      "GSMC_APAC_PR_TEAM__OUTBOUND_MESSAGE",
      "GCCI_SOME__ADS__CAMPAIGN_DESCRIPTION__PAID_INITIATIVE",
      "AD_VARIANT_NAME",
      "GSMC_APAC_ADS__CAMPAIGN_NAME__PAID_INITIATIVE",
      "GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE",
      "PAID_INITIATIVE_NAME",
      "GCCI_SOME__ADS__BRAND_LIFT_STUDY__AD_SET",
      "GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___PAID_INITIATIVE",
      "GCCI_SOME__ADS__BRAND_LIFT_STUDY__PAID_INITIATIVE"
    ]
  },
  "PAID_Tags_2024_13_FluencyWeekly.json": {
    "Number of Columns": 22,
    "Column Names": [
      "GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__PAID_INITIATIVE",
      "IMPRESSIONS__SUM",
      "GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE",
      "AD_ACCOUNT",
      "AD_VARIANT_NAME",
      "GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__PAID_INITIATIVE",
      "AD_POST_PERMALINK",
      "GCCI_SOCIAL_MEDIA_NA__DOES_THIS_INCLUDE_GLOBAL__NON-U_S___CONTENT_ELEMENTS___OUTBOUND_MESSAGE",
      "GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__OUTBOUND_MESSAGE",
      "AD_SET_NAME",
      "GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__WHAT_IS_THE_FORMAT_OF_THIS_POST___OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE",
      "GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__PAID_INITIATIVE",
      "GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___PAID_INITIATIVE",
      "PAID_INITIATIVE_SPRINKLR_SUB-CAMPAIGN"
    ]
  },  
  "Target_Geography_17_FluencyWeekly.json": {
    "Number of Columns": 12,
    "Column Names": [
      "PUBLISHEDTIME",
      "TARGETED_GEOGRAPHY",
      "STATUS",
      "ACCOUNT",
      "ACCOUNT_TYPE",
      "PERMALINK",
      "CAMPAIGN_NAME",
      "OUTBOUND_POST",
      "AUTHOR_NAME",
      "SCHEDULEDTIME",
      "TOTAL_IMPRESSIONS__SUM",
      "TAGS"
    ]
  },
    
"Organic_Tags_12_FluencyMonthly.json":{
"Number of Columns": 37,
"Column Names":['PUBLISHEDTIME',
  'ACCOUNT_TYPE',
  'C_63DAC29E6DF27A45C681FF10',
  'GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE',
  'EVENT_CAMPAIGN_AMAZON_MARKETING___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE',
  '62696511869F0F319C9BB709',
  'GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE',
  'APACPR_OBJECTIVE2__CSV___OUTBOUND_MESSAGE',
  'GCCI_SOME__TIER_1_EVENT_TYPE__MEDIA_ASSET',
  'SUB-CAMPAIGN',
  'STATUS',
  'ACCOUNT',
  'CAMPAIGN_NAME',
  'OUTBOUND_POST',
  'GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE',
  'SCHEDULEDTIME',
  'GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE',
  'CAMPAIGN',
  'C_62D5EEB82AAE8171DF12E8E3',
  'PERMALINK',
  'AUTHOR_NAME',
  '62696512869F0F319C9BB7FD',
  'EVENT_NAME__MESSAGE',
  'TOTAL_IMPRESSIONS__SUM',
  'C_63DAC6AC6DF27A45C687B642',
  'GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE',
  'MEDIA_SOURCE',
  'C_63DAC8FF6DF27A45C68AAF7C',
  'GCCI_SOCIAL_MEDIA_NA__DOES_THIS_INCLUDE_GLOBAL__NON-U_S___CONTENT_ELEMENTS___OUTBOUND_MESSAGE',
  'GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__WHAT_IS_THE_FORMAT_OF_THIS_POST___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE',
  'TAGS',
  'APACPR_OBJECTIVE1__CSV___OUTBOUND_MESSAGE']
 },
 "Organic_Tags_12_TagPull.json":{
"Number of Columns": 37,
"Column Names":['PUBLISHEDTIME',
  'ACCOUNT_TYPE',
  'C_63DAC29E6DF27A45C681FF10',
  'GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE',
  'EVENT_CAMPAIGN_AMAZON_MARKETING___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE',
  '62696511869F0F319C9BB709',
  'GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE',
  'APACPR_OBJECTIVE2__CSV___OUTBOUND_MESSAGE',
  'GCCI_SOME__TIER_1_EVENT_TYPE__MEDIA_ASSET',
  'SUB-CAMPAIGN',
  'STATUS',
  'ACCOUNT',
  'CAMPAIGN_NAME',
  'OUTBOUND_POST',
  'GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE',
  'SCHEDULEDTIME',
  'GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE',
  'CAMPAIGN',
  'C_62D5EEB82AAE8171DF12E8E3',
  'PERMALINK',
  'AUTHOR_NAME',
  '62696512869F0F319C9BB7FD',
  'EVENT_NAME__MESSAGE',
  'TOTAL_IMPRESSIONS__SUM',
  'C_63DAC6AC6DF27A45C687B642',
  'GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE',
  'MEDIA_SOURCE',
  'C_63DAC8FF6DF27A45C68AAF7C',
  'GCCI_SOCIAL_MEDIA_NA__DOES_THIS_INCLUDE_GLOBAL__NON-U_S___CONTENT_ELEMENTS___OUTBOUND_MESSAGE',
  'GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__WHAT_IS_THE_FORMAT_OF_THIS_POST___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE',
  'TAGS',
  'APACPR_OBJECTIVE1__CSV___OUTBOUND_MESSAGE']
},
"Paid_Tags_15_TagPull.json":{
"Number of Columns": 18,
"Column Names":['C_62D5EEB82AAE8171DF12E8E3',
  'SPENT_IN_DEFAULT__SUM',
  'C_63DAC29E6DF27A45C681FF10',
  'EVENT_CAMPAIGN_AMAZON_MARKETING___OUTBOUND_MESSAGE',
  'GCCI_SOME__ADS__AUDIENCE__AD_SET',
  'GCCI_SOME__ADS__CAMPAIGN_DESCRIPTION__AD_SET',
  'C_63DAC6AC6DF27A45C687B642',
  'C_63DAC8FF6DF27A45C68AAF7C',
  'GSMC_APAC_PR_TEAM__OUTBOUND_MESSAGE',
  'GCCI_SOME__ADS__CAMPAIGN_DESCRIPTION__PAID_INITIATIVE',
  'AD_VARIANT_NAME',
  'GSMC_APAC_ADS__CAMPAIGN_NAME__PAID_INITIATIVE',
  'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE',
  'PAID_INITIATIVE_NAME',
  'GCCI_SOME__ADS__BRAND_LIFT_STUDY__AD_SET',
  'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___PAID_INITIATIVE',
  'GCCI_SOME__ADS__BRAND_LIFT_STUDY__PAID_INITIATIVE']
},
 
  
"PAID_Tags_2024_13_TagPull.json":{
"Number of Columns": 22,
"Column Names":['GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__PAID_INITIATIVE',
  'IMPRESSIONS__SUM',
  'GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__IF_THIS_IS_AN_XGC_POST__WHAT_KIND_IS_IT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__CONTENT_SOURCE__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__IS_THIS_POST_CONSIDERED_BREAKING_NEWS___OUTBOUND_MESSAGE',
  'AD_ACCOUNT',
  'AD_VARIANT_NAME',
  'GCCI_SOCIAL_MEDIA__REPUTATIONAL_TOPIC__PAID_INITIATIVE',
  'AD_POST_PERMALINK',
  'GCCI_SOCIAL_MEDIA_NA__DOES_THIS_INCLUDE_GLOBAL__NON-U_S___CONTENT_ELEMENTS___OUTBOUND_MESSAGE',
  'GCCI_SOME__TIER_1_EVENT_TYPE__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__CONTENT_CATEGORY_TYPE_-_INTENT__OUTBOUND_MESSAGE',
  'AD_SET_NAME',
  'GCCI_SOCIAL_MEDIA_NA__IF__WORKPLACE___WHAT_WAS_IT_ABOUT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__WHAT_IS_THE_FORMAT_OF_THIS_POST___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__IF_VIDEO_REEL__HOW_LONG_IS_IT___OUTBOUND_MESSAGE',
  'GCCI_SOCIAL_MEDIA__REQUESTING_PR_ORG__PAID_INITIATIVE',
  'GCCI_SOCIAL_MEDIA_EMEA_APAC__TIER_1_EVENT___PAID_INITIATIVE',"PAID_INITIATIVE_SPRINKLR_SUB-CAMPAIGN"]
 },
 
"Target_Geography_17_TagPull.json":{
"Number of Columns": 12,
"Column Names":['PUBLISHEDTIME',
        'TARGETED_GEOGRAPHY',
        'STATUS',
        'ACCOUNT',
        'ACCOUNT_TYPE',
        'PERMALINK',
        'CAMPAIGN_NAME',
        'OUTBOUND_POST',
        'AUTHOR_NAME',
        'SCHEDULEDTIME',
        'TOTAL_IMPRESSIONS__SUM',
        'TAGS']


},
    
"Target_Geography_17_FluencyMonthly.json":{
"Number of Columns": 12,
"Column Names":['PUBLISHEDTIME',
        'TARGETED_GEOGRAPHY',
        'STATUS',
        'ACCOUNT',
        'ACCOUNT_TYPE',
        'PERMALINK',
        'CAMPAIGN_NAME',
        'OUTBOUND_POST',
        'AUTHOR_NAME',
        'SCHEDULEDTIME',
        'TOTAL_IMPRESSIONS__SUM',
        'TAGS']
  },
    
    
"Tiktok_19_FluencyMonthly.json":
{
  "Number of Columns": 18,
  "Column Names": [
    "PUBLISHEDTIME",
    "ACCOUNT_TYPE",
    "PERMALINK",
    "AUTHOR_NAME",
    "TIKTOK_VIDEO_DURATION",
    "TIKTOK_VIDEO_WATCHED_TO_COMPLETION_RATE",
    "TIKTOK_VIDEO_SHARES",
    "STATUS",
    "ACCOUNT",
    "TIKTOK_VIDEO_LIKES",
    "TIKTOK_VIDEO_COMMENTS",
    "TIKTOK_VIDEO_VIEWS",
    "TIKTOK_VIDEO_REACH",
    "CAMPAIGN_NAME",
    "OUTBOUND_POST",
    "SCHEDULEDTIME",
    "TAGS",
    "MEDIA_TYPE"
  ]
}
 
}
   
  
good_files_daily = [
    'Facebook_Pull_1_FluencyMonthly.json',
    'Instagram_3_FluencyMonthly.json',
    'Instagram_Story_5_FluencyMonthly.json',
    'LinkedIn_6_FluencyMonthly.json',
    'Twitter_2_FluencyMonthly.json',
    'YouTube_10_FluencyMonthly.json',
   'User_Group_Lookup_9_FluencyMonthly.json',
   'Organic_Tags_12_FluencyMonthly.json',
   'Target_Geography_17_FluencyMonthly.json',
   'Tiktok_19_FluencyMonthly.json'
] 
     
# File to check weekly
good_files_weekly = ["Paid_Data_11_FluencyWeekly.json",'Follower_Data_7_FluencyWeekly.json',
                    'Paid_Tags_15_FluencyWeekly.json','PAID_Tags_2024_13_FluencyWeekly.json','Target_Geography_17_FluencyWeekly.json']
 
 
good_tag_files=['Organic_Tags_12_TagPull.json','Paid_Tags_15_TagPull.json','PAID_Tags_2024_13_TagPull.json','Target_Geography_17_TagPull.json']    

  
def send_email(subject, body, email_addresses):
    """Send an email notification using AWS SES."""
   

     
    try:
        ses.send_email(
            Source=email_addresses[0],  # Use the first email address in the list
            Destination={'ToAddresses': email_addresses[1:]},  # Use the rest for "To"
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': body}}
            }
        )
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")


def read_json_lines_from_s3(bucket, key):
    """Fetches a JSON file from S3, reads lines as JSON objects, and returns a DataFrame."""
    response = s3.get_object(Bucket=bucket, Key=key)
    json_lines = response['Body'].read().decode('utf-8').splitlines()
    data = [json.loads(line) for line in json_lines]
    return pd.DataFrame(data)

def list_all_objects(bucket, prefix):
    objects = []
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        objects.extend(response.get('Contents', []))

        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return objects
         
def get_latest_folder(prefix):
    current_date = datetime.utcnow().strftime('%Y-%m-%d')
 
    # List all objects under the given prefix
    objects = list_all_objects(source_bucket, prefix)
    # Initialize variables to track the latest folder
    latest_folder = None
    latest_folder_date = None
    
    for obj in objects:
        # Split the object key to parse the folder structure
        parts = obj['Key'].split('/')
        # Assuming the date is in the third part of the key, following 'RAWDATA/Fluency-*/'
        if len(parts) > 2:  # Check if the path depth is sufficient
            folder_date_str = parts[2].split('_')[0]  # Extract the date part
            try:
                folder_date = datetime.strptime(folder_date_str, '%Y-%m-%d')
                # Compare with the current date
                if folder_date_str == current_date:
                    # Update the latest folder if this is the most recent one
                    folder_path = '/'.join(parts[:3]) + '/'  # Reconstruct the folder path
                    if latest_folder is None or folder_date > latest_folder_date:
                        latest_folder = folder_path
                        latest_folder_date = folder_date
            except ValueError:
                # In case the date parsing fails, skip this object
                continue
    
    print("Latest folder found:", latest_folder)
    return latest_folder
            
def check_files_existence(prefix, required_files):
    if not prefix:
        return False, "No folder found for today's date."
    all_objects = list_all_objects(source_bucket, prefix)

    existing_files = [obj['Key'] for obj in all_objects]
    missing_files = [f for f in required_files if not any(f in s for s in existing_files)]
    
    if missing_files:
        return False, missing_files
    return True, "All files exist."

def get_file_details(latest_folder, files):
    details = {}
    for file_name in files:
        key = f"{latest_folder}{file_name}"
        try:
            df = read_json_lines_from_s3(source_bucket, key)
            details[file_name] = {
                "Number of Columns": len(df.columns),
                "Column Names": df.columns.tolist()
            }
        except Exception as e:
            print(f"Error reading file {file_name}: {str(e)}")
            details[file_name] = "Error reading file"
    return details
     
     
def validate_file_structure(bucket, file_key, file_structure):
    try:
        df = read_json_lines_from_s3(bucket, file_key)
        actual_columns = set(df.columns.tolist())
        expected_columns = set(file_structure["Column Names"])
        missing_columns = expected_columns - actual_columns
        additional_columns = actual_columns - expected_columns

        issues = []
        if len(actual_columns) != file_structure["Number of Columns"]:
            issues.append(f"Column count mismatch: expected {file_structure['Number of Columns']}, found {len(actual_columns)}.")

        if missing_columns:
            issues.append(f"Missing columns: {', '.join(missing_columns)}")
        if additional_columns:
            issues.append(f"Additional columns: {', '.join(additional_columns)}")

        if issues:
            return False, "; ".join(issues)
        return True, "Structure matches"
    except Exception as e:
        return False, f"Error reading file: {str(e)}"
 

def invoke_lambda(function_name, payload):
    """Invoke another Lambda function."""
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response
     
def lambda_handler(event, context):
    validation_issues = []
    email_addresses=['prabhatmishra160@gmail.com','prabhat.mishra@mcsaatchi.com','dlawtonmcsaatchi@gmail.com']

    # Validate daily and weekly files
    for folder_type, files_set in [("Daily", good_files_daily), ("Weekly", good_files_weekly)]:
        latest_folder = get_latest_folder(source_daily_folder if folder_type == "Daily" else source_weekly_folder)
        if not latest_folder:
            validation_issues.append(f"No latest folder found for {folder_type}.")
            continue
        
        _, missing_files = check_files_existence(latest_folder, files_set)
        for file_name in files_set:
            if file_name in missing_files:
                validation_issues.append(f"{folder_type} file: {file_name} - Missing file.")
                continue
            
            file_key = f"{latest_folder}{file_name}"
            is_valid, issue_message = validate_file_structure(source_bucket, file_key, expected_file_structure.get(file_name, {}))
            if not is_valid:
                validation_issues.append(f"{folder_type} file: {file_name} - {issue_message}")

    # If there are issues with daily or weekly files, send an email immediately
    if validation_issues:
        subject = "File Validation Issues Detected"
        body = "The following issues were detected during file validation:\n\n" + "\n".join(validation_issues)
        send_email(subject, body, email_addresses)
        return {
            'statusCode': 400,
            'body': json.dumps({"message": "Validation issues detected with daily or weekly files. Notification sent.", "issues": validation_issues})
        }
 
    # Proceed to validate tag files only if daily and weekly files are valid
    latest_tag_folder = get_latest_folder(source_tag_folder)
    if not latest_tag_folder:
        # If the latest tag folder is not found, invoke Lambda 1 for handling missing tag files
        invoke_lambda(lambda_for_missing_tags, {"message": "Latest tag folder not found"})
        return {
            'statusCode': 200,
            'body': json.dumps({"message": "Latest tag folder not found. Invoked Lambda for missing tag files."})
        }

    # Check existence of tag files in the found folder
    tag_existence_status, missing_tag_files = check_files_existence(latest_tag_folder, good_tag_files)
    if not tag_existence_status or missing_tag_files:
        # If tag files are missing, still consider it as an issue and optionally handle similarly to missing folder
        invoke_lambda(lambda_for_missing_tags, {"message": "Tag files are missing or not all present"})
        return {
            'statusCode': 200,
            'body': json.dumps({"message": "Missing tag files. Invoked Lambda for handling missing tag files."})
        }

    # If the latest tag folder is found and all tag files are present and valid, invoke Lambda 2 for processing tag files
    invoke_lambda(lambda_for_processing_tags, {"message": "All tag files are present and valid"})
    return {
        'statusCode': 200,
        'body': json.dumps({"message": "All tag files validated successfully. Invoked Lambda for processing tag files."})
    }
  
