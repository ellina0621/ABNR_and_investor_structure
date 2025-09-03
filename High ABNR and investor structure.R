# 清除環境
rm(list = ls())
graphics.off()
gc()

#########程式碼##################
# 載入必要套件
library(dplyr)
library(optimx)
library(tidyr)
library(stargazer)
library(PINstimation)
library(purrr)
library(dplyr)
library(lubridate)
library(data.table)
library(tidyr)
library(readxl)
library(TTR) 
library(zoo)
library(glue)
library(purrr)
library(intervals)
library(xgboost)
library(stringr)
library(openxlsx)

setwd("D:/NSYSU FIN/security證券市場微結構/hw4")  

#######日內資料處理##########
# 加載數據
load("mth2019-02.rdata")  
data <- get("mth01_03") 
data <- data %>%
  mutate(MthDate = as.Date(as.character(MthDate), format = "%Y%m%d"))
########日資料處理##########
# 匯入2018-2019年的資料
price <- read_excel("D:/NSYSU FIN/security/AB_NR/20182020123.xlsx")
colnames(price) <- c("code", "date", "open", "high", "low", "close", "return", "turnover", "outstanding", "volume", "P/E")

#########AB_NR#############
# 計算每日的負日間反轉
data <- price %>%
  group_by(code) %>%
  arrange(date) %>%
  mutate(
    RET_CO = open / lag(close) - 1, 
    RET_OC = close / open - 1,   
    is_negative_reversal = ifelse(RET_CO > 0 & RET_OC < 0, 1, 0) 
  )
# 每月最後一日的收盤價
monthly_close <- data %>%
  mutate(month = substr(as.character(date), 1, 6)) %>%  # 提取年月
  group_by(code, month) %>%
  filter(date == max(date)) %>% 
  summarise(last_close = close)  

monthly_NR <- data %>%
  mutate(month = substr(as.character(date), 1, 6)) %>%  # 提取年月
  group_by(code, month) %>%
  summarise(
    NR = sum(is_negative_reversal, na.rm = TRUE) / n()  # 負日間反轉天數 / 總交易天數
  )
# 計算過去 12 個月平均 NR
monthly_NR <- monthly_NR %>%
  group_by(code) %>%
  arrange(month) %>%
  mutate(
    baseline_NR = zoo::rollmean(NR, k = 12, fill = NA, align = "right")  # 過去 12 個月平均
  )
# 計算 AB_NR
monthly_NR <- monthly_NR %>%
  mutate(
    AB_NR = ifelse(!is.na(baseline_NR) & baseline_NR != 0, NR / baseline_NR, NA)
  )

merged_data <- monthly_close %>%
  left_join(monthly_NR, by = c("code", "month"))


cleaned_data <- merged_data %>%
  ungroup() %>%  # 取消分組
  filter(complete.cases(.))  # 刪除包含 NA 的列


######觀察2019-10&11哪些股票的AB_NR較高#######
# 篩選 2019 年 10 月和 2019 年 11 月的資料
filtered_data <- cleaned_data %>%
  filter(month %in% c("201910", "201911"))

#head(filtered_data)

# ，將 AB_NR 分成 10 組（分位數分組）
grouped_data <- filtered_data %>%
  group_by(month) %>%
  mutate(AB_NR_Group = ntile(AB_NR, 10)) 

# AB_NR 最高的
highest_group_data <- grouped_data %>%
  filter(AB_NR_Group == 10) %>%
  arrange(month, desc(AB_NR))

head(highest_group_data)

highest_group_data <- highest_group_data %>%
  mutate(code = str_extract(code, "\\d+"))

###提取較高組別的日內資料##########

highest_group_data <- highest_group_data %>%
  mutate(code = as.character(code))

filtered_intraday_data <- mth01_03 %>%
  mutate(StkNo = as.character(StkNo)) %>%
  filter(substr(MthDate, 1, 6) %in% c("201910", "201911")) %>%
  semi_join(highest_group_data, by = c("StkNo" = "code"))


head(filtered_intraday_data)

#####夜間報酬與日內反轉DUMMY設置####
price <- price %>%
  mutate(code = str_extract(code, "\\d+"))

highest_group_data <- highest_group_data %>%
  mutate(code = as.character(code))

price_filtered <- price %>%
  mutate(code = as.character(code)) %>%
  semi_join(highest_group_data, by = "code")

# 計算夜間報酬和日內反轉
price_filtered <- price_filtered %>%
  group_by(code) %>%
  arrange(date) %>%
  mutate(
    RET_CO = open / lag(close) - 1, 
    RET_OC = close / open - 1,       
    Night_Return_Dummy = ifelse(RET_CO > 0, 1, 0),  
    Intraday_Reversal_Dummy = ifelse(RET_OC < 0, 1, 0) 
  )


price_filtered <- price_filtered %>%
  filter(date >= 20191001 & date <= 20191130) 

#######處理日內資料開收盤成交量########
# 開盤成交量 (9000000 ~ 9300000)
opening_volume <- filtered_intraday_data %>%
  filter(MthTime >= 9000000 & MthTime < 9300000) %>%
  group_by(MthDate, StkNo, InvestType) %>%
  summarise(
    Opening_Volume = sum(MthShr, na.rm = TRUE),
    Opening_Buy_Count = sum(BuySell == "B"),
    Opening_Sell_Count = sum(BuySell == "S"),
    .groups = 'drop'
  )

# 盤中成交量 (9300000 ~ 13200000)
midday_volume <- filtered_intraday_data %>%
  filter(MthTime >= 9300000 & MthTime < 13200000) %>%
  group_by(MthDate, StkNo, InvestType) %>%
  summarise(
    Midday_Volume = sum(MthShr, na.rm = TRUE),
    Midday_Buy_Count = sum(BuySell == "B"),
    Midday_Sell_Count = sum(BuySell == "S"),
    .groups = 'drop'
  )

# 收盤成交量 (13200000 ~ 13300000)
closing_volume <- filtered_intraday_data %>%
  filter(MthTime >= 13200000 & MthTime < 13300000) %>%
  group_by(MthDate, StkNo, InvestType) %>%
  summarise(
    Closing_Volume = sum(MthShr, na.rm = TRUE),
    Closing_Buy_Count = sum(BuySell == "B"),
    Closing_Sell_Count = sum(BuySell == "S"),
    .groups = 'drop'
  )
# 彙總開盤成交量
opening_summary <- opening_volume %>%
  group_by(MthDate, StkNo) %>%
  summarise(
    Opening_Retail_Volume = sum(Opening_Volume[InvestType == "I"], na.rm = TRUE),
    Opening_Institutional_Volume = sum(Opening_Volume[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    Opening_Retail_Buy_Count = sum(Opening_Buy_Count[InvestType == "I"], na.rm = TRUE),
    Opening_Retail_Sell_Count = sum(Opening_Sell_Count[InvestType == "I"], na.rm = TRUE),
    Opening_Institutional_Buy_Count = sum(Opening_Buy_Count[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    Opening_Institutional_Sell_Count = sum(Opening_Sell_Count[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    .groups = 'drop'
  )

# 彙總盤中成交量
midday_summary <- midday_volume %>%
  group_by(MthDate, StkNo) %>%
  summarise(
    Midday_Retail_Volume = sum(Midday_Volume[InvestType == "I"], na.rm = TRUE),
    Midday_Institutional_Volume = sum(Midday_Volume[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    Midday_Retail_Buy_Count = sum(Midday_Buy_Count[InvestType == "I"], na.rm = TRUE),
    Midday_Retail_Sell_Count = sum(Midday_Sell_Count[InvestType == "I"], na.rm = TRUE),
    Midday_Institutional_Buy_Count = sum(Midday_Buy_Count[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    Midday_Institutional_Sell_Count = sum(Midday_Sell_Count[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    .groups = 'drop'
  )

# 彙總收盤成交量
closing_summary <- closing_volume %>%
  group_by(MthDate, StkNo) %>%
  summarise(
    Closing_Retail_Volume = sum(Closing_Volume[InvestType == "I"], na.rm = TRUE),
    Closing_Institutional_Volume = sum(Closing_Volume[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    Closing_Retail_Buy_Count = sum(Closing_Buy_Count[InvestType == "I"], na.rm = TRUE),
    Closing_Retail_Sell_Count = sum(Closing_Sell_Count[InvestType == "I"], na.rm = TRUE),
    Closing_Institutional_Buy_Count = sum(Closing_Buy_Count[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    Closing_Institutional_Sell_Count = sum(Closing_Sell_Count[InvestType %in% c("M", "F", "J")], na.rm = TRUE),
    .groups = 'drop'
  )

# 合併成交量和買賣次數
merged_volumes <- opening_summary %>%
  left_join(midday_summary, by = c("MthDate", "StkNo")) %>%
  left_join(closing_summary, by = c("MthDate", "StkNo"))

# 查看結果
head(merged_volumes)


######合併資料##########
merged_data <- merge(
  merged_volumes,
  price_filtered,
  by.x = c("StkNo", "MthDate"),
  by.y = c("code", "date"),
  all.x = TRUE
)

# 查看合併後的資料
head(merged_data)


####設置開收盤dummy##########
# 新增開盤成交量 Dummy
merged_data <- merged_data %>%
  mutate(
    # 開盤散戶成交量大於法人成交量
    Opening_Retail_Dummy = ifelse(Opening_Retail_Volume > Opening_Institutional_Volume, 1, 0),
    
    # 盤中法人成交量大於散戶成交量
    Midday_Institutional_Dummy = ifelse(Midday_Institutional_Volume > Midday_Retail_Volume, 1, 0),
    
    # 收盤法人成交量大於散戶成交量
    Closing_Institutional_Dummy = ifelse(Closing_Institutional_Volume > Closing_Retail_Volume, 1, 0),
    
    # 新增開盤散戶買單比法人買單多
    Opening_Retail_Buy_Dummy = ifelse(Opening_Retail_Buy_Count > Opening_Institutional_Buy_Count, 1, 0),
    
    # 新增盤中法人賣單比散戶賣單多
    Midday_Institutional_Sell_Dummy = ifelse(Midday_Retail_Sell_Count < Midday_Institutional_Sell_Count, 1, 0),
    
    # 新增收盤法人賣單比散戶賣單多
    Closing_Institutional_Sell_Dummy = ifelse(Closing_Retail_Sell_Count < Closing_Institutional_Sell_Count, 1, 0)
  )


merged_data <- merged_data %>%
  mutate(
    Interaction_Term_1 = Night_Return_Dummy * Opening_Retail_Dummy ,
    Interaction_Term_3 = Opening_Retail_Dummy * Opening_Retail_Buy_Dummy,
    Interaction_Term_4 = Midday_Institutional_Dummy * Midday_Institutional_Sell_Dummy
  )

# 建立迴歸模型
model <- lm(
  Intraday_Reversal_Dummy ~ Interaction_Term_1 +Interaction_Term_3+Interaction_Term_4+
    Night_Return_Dummy + Opening_Retail_Dummy + Midday_Institutional_Dummy +Midday_Institutional_Sell_Dummy+
    Opening_Retail_Buy_Dummy+ factor(StkNo) + factor(MthDate),
  data = merged_data
)

summary(model)

########匯出model###########
stargazer(
  model,
  type = "html",
  title = "日內反轉回歸分析結果",
  out = "Intraday_Reversal_Regression.doc",
  dep.var.labels = "Intraday Reversal Dummy",
  covariate.labels = c(
    "Night Return * Opening Retail",
    "Combined Interaction",
    "Night Return Dummy",
    "Opening Retail Dummy",
    "Midday Institutional Dummy",
    "Midday Institutional Sell Dummy",
    "Opening Retail Buy Dummy"
  ),
  add.lines = list(
    c("Firm Fixed Effects", "Yes"),
    c("Time Fixed Effects", "Yes")
  ),
  omit = c("factor\\(StkNo\\)", "factor\\(MthDate\\)"),
  digits = 3
)
