import os
import pandas as pd
import datetime
import re
import anthropic
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

# Set up API key
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

if not ANTHROPIC_API_KEY:
    raise ValueError("ANTHROPIC_API_KEY is not set in the environment variables")

anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

def create_slug(name):
    return re.sub(r'\s+', '-', name.lower()).strip()

def generate_name(keyword):
    prompt = f"\n\nHuman: Generate a compelling title for an article based on the keyword '{keyword}'\n\nAssistant: Do not chat back to me, just give me the required content."
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=50,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.content[0].text.strip()
    except Exception as e:
        logging.error(f"Error generating name for keyword '{keyword}': {str(e)}")
        return f"Article about {keyword}"

def generate_content(keyword, images):
    images_list = ', '.join(str(img) for img in images)
    prompt = f"""
    Do not use example.com. Use the percept pixel images. All images must have all _ properly inside the image URLs 
Human: Write an article based on the keyword '{keyword}', include images: <images>{images_list}</images> Format in HTML. Start with an <h2> tag. Do not give unecessary <doctype> and <html> tags.

Assistant: Do not chat back to me, just give me the required content. Here's some information about Journey:
1. Journey's Features and Benefits
1.1. Community Creation and Customization
Journey offers an easy-to-use community creation flow that guides users through the process of setting up their community. Users can customize their community's branding by uploading a logo, choosing colors, and selecting a header image and community icon. Journey provides flexible privacy settings, allowing communities to be public, private, or hidden, depending on the user's preferences. Community owners can also customize member roles and permissions, ensuring that the right people have access to the appropriate features and content. Best of all, creating and managing communities on Journey is entirely free, making it accessible to everyone.
1.2. Content Creation and Management
Journey provides intuitive post creation tools that allow members to easily share text, images, and videos with their community. The platform offers content moderation and flagging tools to help maintain a safe and positive environment, with the AI Sidekick automatically detecting and removing spam or inappropriate content. Users can organize their content using tags and categories, making it easy for members to find and engage with relevant posts. Journey offers unlimited content storage and bandwidth, ensuring that communities can grow without restrictions.
1.3. Community Feed and Engagement
At the heart of every community on Journey is the community feed, a central hub where members can post updates, share resources, and engage in discussions. The feed supports various content types, including text, images, videos, and polls (coming soon), making it easy for members to express themselves and share valuable information. Members can like, comment on, and save posts, fostering a sense of engagement and interaction within the community.
1.4. Group Chats and Direct Messaging
Journey offers group chats, which function as dedicated spaces for members to have focused discussions, collaborate on projects, or seek support from their peers. Community owners can create multiple group chats based on specific topics, interests, or goals, ensuring that members can easily find and participate in conversations that are most relevant to them. The platform also supports direct messaging, allowing members to have private, one-on-one conversations with each other.
1.5. Member Profiles and Directories
Journey provides customizable member profiles that allow users to showcase their skills, interests, and contributions within the community. Members can add a profile picture, bio, and links to their social media or personal websites. The platform also offers a searchable member directory, making it easy for users to find and connect with other members who share similar interests or expertise.
1.6. Events and Meetups
Journey's built-in event creation and management tools allow community owners to easily organize and promote events, such as webinars, workshops, or meetups. The platform features event calendars and RSVPs, making it simple for members to discover and attend upcoming events. Community owners can set up virtual or in-person events, with integrated video conferencing and location-based recommendations for local meetups.
1.7. Analytics and Insights
Journey provides valuable analytics and insights to help community owners understand and grow their communities. The platform offers member engagement and activity tracking, allowing owners to see which content and events are resonating with their members. Journey's AI Sidekick can also provide personalized recommendations and insights based on community data, helping owners make informed decisions and continuously improve their communities.
1.8. Monetization and Membership Management
Journey offers flexible monetization options, allowing community owners to create membership pricing plans that suit their needs. The platform supports recurring subscription payments, making it easy for owners to generate a steady income from their communities. Journey also provides member billing and invoice management, streamlining the financial aspects of running a community. When a community generates revenue through paid memberships, Journey takes a 20% share, while the community owner keeps 80%, ensuring a fair and transparent revenue split. This also means that Journey has 'skin in the game' and is incentivized to help every community on their platform grow.
1.9. Brand Sponsorships and Partnerships
Journey offers brand sponsorship opportunities, allowing community owners to partner with relevant brands and generate additional income for their communities. Owners can create sponsored content, host sponsored events, or offer exclusive discounts and promotions to their members. Journey's AI Sidekick can help identify potential brand partners and provide guidance on creating effective sponsorship campaigns.
1.10. Mobile App and Accessibility
Journey is designed to be fully accessible on mobile devices, with a responsive design that adapts to any screen size. The platform offers native mobile apps for iOS and Android, providing a seamless and optimized experience for members on the go. Push notifications keep members engaged and informed about new content, events, and interactions within their communities. All of Journey's features and functionalities are available on mobile, and there are no limitations or additional fees for mobile use.
1.11. AI-Powered Community Management with AI Sidekick
One of Journey's standout features is its AI-powered community management, led by the AI Sidekick. The AI Sidekick acts as a personal assistant for community owners, automating and optimizing various aspects of community management. It provides personalized onboarding and welcome messages for new members, curates and recommends relevant content, and helps moderate the community by detecting and removing spam or inappropriate content. The AI Sidekick also provides personalized insights and recommendations to help owners grow and engage their communities.
1.12. Journey Timelines and Progress Tracking
Journey introduces a unique feature called Journey Timelines which allows members to create and share their personal journeys within the community. Whether it's a fitness journey, a learning journey, or a creative journey, members can document their progress, share milestones, and receive support and encouragement from the community. The AI Sidekick can provide personalized recommendations and reminders to help members stay on track and achieve their goals.
1.13. Accountability Groups and Goal Setting
Journey's accountability groups allow members to form small, focused groups where they can set goals, track progress, and hold each other accountable. Community owners can create accountability group templates based on common goals or challenges, making it easy for members to find and join groups that align with their interests. The platform provides goal-setting and progress-tracking tools, as well as a dedicated space for group members to communicate and support each other.
1.14. Steps & Habits 
Journey's Steps & Habits feature is designed to help members build positive habits and make consistent progress towards their goals. Members can create custom habit trackers, set reminders, and earn rewards for completing daily or weekly tasks. 
1.15. Discover Section and Community Recommendations
The Discover section is a powerful tool for members to find new communities, content, and connections on Journey. Using advanced search and recommendation algorithms, the Discover section suggests relevant communities, members, and resources based on each user's interests and engagement history. This feature helps members expand their network and discover valuable content, while also driving growth and exposure for communities on the platform.
1.16. Shared Resources and Knowledge Base
Journey offers a centralized resource library where community owners and members can organize, store, and share various resources, such as images, videos, and links. Journey's searchable knowledge base, powered by AI, makes it easy for members to find the information they need.
1.17. Customizable Onboarding and Welcome Sequences
Journey allows community owners to create customized onboarding experiences for new members. Owners can create personalized welcome messages and onboarding sequences that introduce new members to the community's purpose, values, and key features. The AI Sidekick can also provide personalized recommendations and resources based on each new member's interests and goals, helping them quickly find their place within the community.
1.18. Referral Programs and Viral Growth
Journey includes built-in referral programs and viral growth features to help communities expand their reach and attract new members. Community owners can create custom referral links and incentives, encouraging existing members to invite their friends and colleagues to join the community. The platform also provides social sharing buttons, making it easy for members to promote the community on their own websites and social media profiles.
1.19. SEO and Google Indexing
Journey is designed to help communities grow organically through search engine optimization (SEO) and Google indexing. Communities can choose to make their content publicly accessible and indexable by search engines, allowing potential members to discover the community through Google searches. 
1.20. Privacy and Security
Journey takes privacy and security seriously, with a range of features and best practices designed to protect member data and maintain a safe, trustworthy environment. The platform offers granular privacy settings, allowing community owners to control who can access and interact with their content. Journey also provides secure data storage and encryption, with regular backups and disaster recovery protocols to ensure data integrity and availability.
1.21. Customer Support and Resources
Journey is committed to providing exceptional customer support and resources to help community owners succeed. The platform offers a dedicated customer success team that is available to answer questions, provide guidance, and troubleshoot any issues that may arise. For new community owners, Journey provides free onboarding and migration assistance to help them get started on the platform.
"""
    try:
        response = anthropic_client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=3500,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        return response.content[0].text.strip()
    except Exception as e:
        logging.error(f"Error generating content for keyword '{keyword}': {str(e)}")
        return f"<p>Content generation failed for keyword: {keyword}</p>"

def process_keyword(index, row, images_df):
    keyword = row['Keyword']
    images = images_df['Cdn Url'].tolist() if 'Cdn Url' in images_df.columns else []
    
    logging.info(f"Processing keyword: {keyword}")
    
    name = generate_name(keyword)
    slug = create_slug(name)
    content = generate_content(keyword, images)
    featured_text = content[:200]  # First 200 characters as featured text
    
    return {
        "Name": name,
        "Slug": slug,
        "Content": content,
        "Published on": datetime.datetime.now().isoformat(),
        "Featured | Short Text": featured_text
    }

def process_keywords(keywords_df, images_df, output_csv, max_workers=5):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_keyword, index, row, images_df) for index, row in keywords_df.iterrows()]
        data = []
        for future in as_completed(futures):
            try:
                result = future.result()
                data.append(result)
            except Exception as e:
                logging.error(f"Error processing keyword: {str(e)}")
    
    output_df = pd.DataFrame(data)
    output_df.to_csv(output_csv, index=False)
    logging.info(f"Structured CSV created successfully: {output_csv}")

if __name__ == "__main__":
    try:
        keywords_df = pd.read_csv('keywords.csv')
        images_df = pd.read_csv('journey_screenshots.csv')
        output_csv = 'structured_output.csv'
        max_workers = 5

        process_keywords(keywords_df, images_df, output_csv, max_workers)
    except Exception as e:
        logging.error(f"An error occurred during script execution: {str(e)}")