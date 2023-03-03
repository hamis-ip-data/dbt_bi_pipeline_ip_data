{{
  config(
    materialized = 'table',
    )
}}

  select
    Date,
    Data_Source_type,
    Data_Source_id,
    Cost,
    Clicks,
    Impressions,
    Media_type,
    Campaign,
    Data_Source_name,
    Data_Source,

    /* Doubleclick search data */
    Account__DoubleClick_Search,
    Account_ID__DoubleClick_Search,
    Ad__DoubleClick_Search,
    Ad_group__DoubleClick_Search,
    Ad_group_ID__DoubleClick_Search,
    Ad_ID__DoubleClick_Search,
    Advertiser_ID__DoubleClick_Search,
    Advertiser__DoubleClick_Search,
    Agency_ID__DoubleClick_Search,
    Agency__DoubleClick_Search,
    Campaign_ID__DoubleClick_Search,
    Campaign__DoubleClick_Search,
    Engine__DoubleClick_Search,
    Actions__DoubleClick_Search,
    AdWords_conversions__DoubleClick_Search,
    AdWords_VT_conversions__DoubleClick_Search,
    AdWords_conversion_value__DoubleClick_Search,
    Clicks__DoubleClick_Search,
    Cost__DoubleClick_Search,
    Impressions__DoubleClick_Search,
    Transactions__DoubleClick_Search,
    Visits__DoubleClick_Search,

    /* Linkedin data */     
    Creative_Name__LinkedIn,
    Creative_ID__LinkedIn,
    Creative_Type__LinkedIn,
    Creative_Title__LinkedIn,
    Ad_Type__LinkedIn,
    Sponsored_Content_Name__LinkedIn,
    Sponsored_Content_Text__LinkedIn,
    Sponsored_Content_Title__LinkedIn,
    Video_Name__LinkedIn,
    Text_Ad_Text__LinkedIn,
    Text_Ad_Title__LinkedIn,
    Spotlight_Name__LinkedIn,
    Spotlight_Text__LinkedIn,
    Video_Title__LinkedIn,
    Action_Clicks__LinkedIn,
    Clicks__LinkedIn,
    Card_Clicks__LinkedIn,
    Ad_Unit_Clicks__LinkedIn,
    Comment_Likes__LinkedIn,
    Company_Page_Clicks__LinkedIn,
    Comments__LinkedIn,
    Impressions__LinkedIn,
    Follows__LinkedIn,
    Full_Screen_Plays__LinkedIn,
    Likes__LinkedIn,
    One_Click_Lead_Form_Opens__LinkedIn,
    One_Click_Leads__LinkedIn,
    Other_Engagements__LinkedIn,
    Opens__LinkedIn,
    Post_Reactions__LinkedIn,
    Sends__LinkedIn,
    Spend__LinkedIn,
    Shares__LinkedIn,
    Total_Engagements__LinkedIn,
    Video__50__LinkedIn,
    Video__25__LinkedIn,
    Video__75__LinkedIn,
    Video_Completions__LinkedIn,
    Video_Starts__LinkedIn,
    Video_Views__LinkedIn,
    Viral_Card_Clicks__LinkedIn,
    Viral_Clicks__LinkedIn,
    Viral_Comments__LinkedIn,
    Viral_Video_Starts__LinkedIn,
    Viral_Video__25__LinkedIn,
    Viral_Video__50__LinkedIn,
    Viral_Video__75__LinkedIn,
    Viral_Total_Engagements__LinkedIn,
    Viral_Shares__LinkedIn,
    Viral_One_Click_Leads__LinkedIn,
    Account__LinkedIn,
    Account_ID__LinkedIn,
    Campaign__LinkedIn,
    Campaign_End_Date__LinkedIn,
    Campaign_Format__LinkedIn,
    Campaign_Group__LinkedIn,
    Campaign_Group_ID__LinkedIn,
    Campaign_ID__LinkedIn,
    Campaign_Objective__LinkedIn,
    Campaign_Start_Date__LinkedIn,
    Campaign_Type__LinkedIn,



    /* Snapchat data */     
    Account_ID__Snapchat,
    Ad_account_name__Snapchat,
    Ad_ID__Snapchat,
    Ad_Name__Snapchat,
    Ad_Type__Snapchat,
    Campaign_ID__Snapchat,
    Campaign_Name__Snapchat,
    Campaign_Objective__Snapchat,
    Impressions__Snapchat,
    Leads__Snapchat,
    Saves__Snapchat,
    Spend__Snapchat,
    Shares__Snapchat,
    Conversion_Cost__LinkedIn,
    Squad_Name__Snapchat,
    Story_completes__Snapchat,
    Swipes__Snapchat,
    Story_Opens__Snapchat,
    Video_Views_at_25__Snapchat,
    Video_Views_time_based__Snapchat,
    Video_Views_at_50__Snapchat,
    Video_Views_at_75__Snapchat,
    View_Completion__Snapchat,
    Shares_Total_View__Snapchat,
    Searches_Total_View__Snapchat,
    Save_Total_View__Snapchat,
    Reservations_Total_View__Snapchat,

    /* Facebook Ads data */         
    Ad_Account_ID__Facebook_Ads,
    Ad_ID__Facebook_Ads,
    Ad_Name__Facebook_Ads,
    Ad_Set_Name__Facebook_Ads,
    Campaign_Name__Facebook_Ads,
    Campaign_ID__Facebook_Ads,
    Ad_Set_ID__Facebook_Ads,
    Donate_Conversion_Value__Facebook_Ads,
    Leads_Conversion_Value__Facebook_Ads,
    Adds_to_Cart__Facebook_Ads,
    Adds_to_Wishlist__Facebook_Ads,
    Contacts__Facebook_Ads,
    Content_Views__Facebook_Ads,
    Donations__Facebook_Ads,
    Leads__Facebook_Ads,
    Levels_Achieved__Facebook_Ads,
    Location_Searches__Facebook_Ads,
    Registrations_Completed__Facebook_Ads,
    Ratings_Submitted__Facebook_Ads,
    Purchases__Facebook_Ads,
    Tutorials_Completed__Facebook_Ads,
    Link_Clicks__Facebook_Ads,
    Outbound_Clicks__Facebook_Ads,
    n_3_Second_Video_Views__Facebook_Ads,
    Amount_Spent__Facebook_Ads,
    Clicks_all__Facebook_Ads,
    Impressions__Facebook_Ads,
    Reach___1_Day_Campaign__Facebook_Ads,
    Frequency___1_Day_Campaign__Facebook_Ads,

    /* tiktok data */     
    Ad_ID__TikTok,
    Ad_name__TikTok,
    Ad_text__TikTok,
    Adgroup_ID__TikTok,
    Adgroup_interest__TikTok,
    Adgroup_name__TikTok,
    Advertiser_name__TikTok,
    Advertiser_ID__TikTok,
    Campaign_ID__TikTok,
    Campaign_name__TikTok,
    Clicks__TikTok,
    Conversions__TikTok,
    Impressions__TikTok,
    Total_cost__TikTok,
    Video_ID__TikTok,
    CTA_conversions__TikTok,
    CTA_Purchase__TikTok,
    CTA_Registration__TikTok,
    Anchor_Clicks__TikTok,
    Likes__TikTok,
    Install__TikTok,
    Purchases__TikTok,
    Real_time_App_Installs__TikTok,
    Total_Achieve_Level_In_App_Event__TikTok,

    /* Twitter data */     
    Ad_Group__Twitter,
    Ad_Group_ID__Twitter,
    Ad_Name__Twitter,
    Campaign__Twitter,
    Campaign_ID__Twitter,
    Custom_Conversions__Twitter,
    Custom_Post_View__Twitter,
    Custom_Sale_Amount__Twitter,
    Custom_Sale_Amount_View__Twitter,
    Downloads__Twitter,
    Downloads_Order_Quantity__Twitter,
    Downloads_Post_View__Twitter,
    Downloads_Sale_Amount__Twitter,
    Purchases_Post_View__Twitter,
    Purchases_Sale_Amount__Twitter,
    Purchases_Post_Engagement__Twitter,
    Sign_Ups__Twitter,
    Sign_Ups_Order_Quantity__Twitter,
    Sign_Ups_Order_Quantity_Engagement__Twitter,
    Sign_Ups_Order_Quantity_View__Twitter,
    Sign_Ups_Post_View__Twitter,
    Site_Visits__Twitter,
    Site_Visits_Order_Quantity__Twitter,
    Site_Visits_Order_Quantity_Engagement__Twitter,
    Carousel_Swipes__Twitter,
    Clicks__Twitter,
    Engagements__Twitter,
    Follows__Twitter,
    Impressions__Twitter,
    Likes__Twitter,
    Link_Clicks__Twitter,
    Retweets__Twitter,
    Replies__Twitter,
    Media_Views__Twitter,
    Spend__Twitter

  from {{ source('gae_ds_data', 'gae_ds_export_*') }}
