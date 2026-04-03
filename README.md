# GA4 E-commerce Funnel Analysis

## 📊 Project Overview
This project analyzes the user journey and conversion funnel of an e-commerce website using Google Analytics 4 (GA4) data in BigQuery.

The goal is to identify where users drop off in the funnel and uncover the main factors affecting conversion performance.

---

## 🛠️ Tools & Technologies
- Google BigQuery (SQL)
- Tableau Public
- Google Analytics 4 dataset

---

## 🧠 Methodology

### Data Preparation
- Transformed GA4 event-level data into a session-based dataset
- Combined `user_pseudo_id` and `ga_session_id` to create session-level granularity
- Extracted landing page, traffic source, and device information

### Funnel Analysis
- session_start → view_item → add_to_cart → begin_checkout → purchase
- Calculated conversion rates between each step

### Segmentation
- Landing pages
- Traffic sources
- Device categories
- Operating systems
- Device languages

---

## 📈 Key Insights

- Only ~20% of users reach product pages
- The largest drop-off occurs before product interaction
- Homepage and category pages have high traffic but low conversion
- Device analysis shows no significant difference
- Conversion issues are driven by UX and product discovery

---

## 📊 Dashboard

![Dashboard](dashboard/dashboard.png)

👉 Tableau Public Link: https://public.tableau.com/views/E-commerceFunnelConversionAnalysis_17751356288040/Dashboard1?:language=en-US&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

---

## 📁 SQL Queries

- Ana Tablo Link: https://console.cloud.google.com/bigquery?sq=274895270858:7485140564834a78bebb9edaeede5199 
- KPI Query: https://console.cloud.google.com/bigquery?sq=274895270858:b3effe8b84e14a07ac74a7b50fcceadc
- Funnel Query: https://console.cloud.google.com/bigquery?sq=274895270858:4bd443d1375640daa63d1108f4d390fb
- Landing Page Query: https://console.cloud.google.com/bigquery?sq=274895270858:d4f2ca6b32c740c4885298081c71511e
- Device Query: https://console.cloud.google.com/bigquery?sq=274895270858:0b8eedd5251f48e9b901e8ad39376286
- Traffic Link: https://console.cloud.google.com/bigquery?sq=274895270858:292af782b2b84c9a880c547db468a44d

---

## 💡 Business Recommendations

- Improve product visibility on landing pages
- Optimize navigation for faster product discovery
- Enhance user experience to reduce early drop-offs
