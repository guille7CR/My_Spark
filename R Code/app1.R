
# Libraries and resources

library(shiny)
library(shinyWidgets)
library(shinydashboard)
library(ggplot2)
library(plyr)
library(dplyr)
library(sjmisc)
library(shinythemes)
library(DT)
library(rpivotTable)
library(ECharts2Shiny)
library(shinySearchbar)
options(shiny.maxRequestSize=30*1024^2)



# Read the datasets and make it persistent 

transactions <- read.csv("~/DaD R Shiny/Transactions.csv")

structural <- read.csv("~/DaD R Shiny/Structural.csv")

analysis <- read.csv("~/DaD R Shiny/Analysis.csv")

df_summary <-
  analysis                          %>% # Pipe df into group_by
  group_by(SUPPLIER_NM)              %>% # grouping by 'SUPPLIER_NM' column
  summarise(name_count = n()) %>% # calculate the name count for each group
  top_n(n=5)

## 'df_summary' now contains the summary data for each 'type'

df_summary

  

# Beginning of the App


  
ui <- dashboardPage(


  

  #Dashboard Header
  
  
  dashboardHeader(title = "DaD Dashboard", titleWidth = 295,
                
                  
                  dropdownMenu(type = "message", 
                               messageItem(from = "Transaction update",message = "We are in shape here", icon = icon("book")),
                               messageItem(from = "Analysis update",message = "We are in shape here", icon = icon("bar-chart"), time="22:00"),
                               messageItem(from = "Structural update",message = "We are in shape here", icon = icon("cubes"))
                               
                  )),
  
  #Dashboard SideBar
  
  dashboardSidebar( width = 295,
                    
    img(src = "exp_logo.png", height = 60, width = 295),
    
    sidebarMenu(
      
      searchInput(
        inputId = "search", label = "Enter your web search",
        placeholder = "Enter an address",
        btnSearch = icon("search"),
        btnReset = icon("remove"),
        width = "450px"
      ),
      br(),
      verbatimTextOutput(outputId = "res"),
    
      
      
      menuItem("Dashboard", icon = icon("tachometer"), tabName="dashboard"),
      
      
      menuItem("Transactions", icon = icon("book"),
               menuSubItem("Transactions Data", tabName = "trn"),
               menuSubItem("Section2 ")),
      
      menuItem("Structural",icon = icon("cubes"),
               menuSubItem("Structural Data", tabName = "str"),
               menuSubItem("Section2 ")),
      
      menuItem("Analysis1",icon = icon("binoculars"),
               menuSubItem("Analysis Data", tabName = "anls1"),
               menuSubItem("Section2 ")),
      
      menuItem("Analysis",icon = icon("binoculars"),
               menuSubItem("Analysis Data", tabName = "anls"),
               menuSubItem("Section2 ")),
      
      menuItem("Pivot ",icon = icon("eye"),tabName = "pvt"),
      
      sliderInput("bins","Number of Breaks", 1,100,50),
     
      
      
      selectInput(inputId = "bincolor",
                  label = "Select bin color",
                  choices = c("orange", "blue", "green", "maroon", "grey")
      ),
      
      
      selectInput("column",
                  "Elements",
                  paste(transactions$COLUMN_NM), 
                  selected = "Name", multiple = FALSE)
    
      )),
  
  #Dashboard Body
  
  dashboardBody(
    
    tags$head(
      tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")),
    
    tabItems(
      
         
      tabItem("dashboard",
              
              
              fluidRow(
                
                infoBoxOutput("trnPercent"),
                infoBox("Dummy Metrics", paste0('20%'),icon=icon("cubes"), color = "orange"),
                infoBox("Dummy Metrics", paste0('10%'),icon=icon("pie-chart"), color = "orange"),
           
               
                
              ),
              
              
              fluidRow(
                valueBoxOutput("trans"),
                valueBoxOutput("struc"),
                valueBoxOutput("analys"),
                
              ),
              
              
              
              h4(strong("Data Elements in Analysis Df")),
              br(),br(),
              loadEChartsLibrary(),
              tags$div(id="test", style="width:40%;height:200px;"),
              deliverChart(div_id = "test"),
              
              
             box(width = 500,
              column(width = 6,
              fluidRow(
                  tabBox( width = 12,
                  tabPanel(title = "Histogram", status="primary", solidHeader=TRUE,
                           plotOutput("histogram")),
                  
                  tabPanel(title = "Controls of Dashboard", status="warning", solidHeader=TRUE,
                           "Use these controls to fine tune your dashboard",br(), br(),
                           "Do not use lot of controls as it confuses the user",
                            br(),
                           sliderInput("bins", "Number of breaks", 1,100,50)
                         
                   )
                 )
               )
            ),
            
              
              column(width = 6,
              
              fluidRow(
                  tabBox(width = 12,
                    tabPanel(title = "Distribution by Element", status="primary", solidHeader=TRUE,
                           plotOutput("elementPlot"))
              )
            )
          )
        ) 
      ),
      
      tabItem("str",
              
              fluidPage(
                h1("Structural"),
                br(),
                helpText("Click the column header to sort a column."),
                sidebarLayout(
                sidebarPanel(
                  conditionalPanel(
                    'input.dataset === "structural"',
                       checkboxGroupInput("show_vars", "Columns in structural to show:",
                           names(structural), selected = names(structural))  
                  )
                ),
                
                mainPanel(
                  tabsetPanel(
                    id = 'dataset',
                    tabPanel("structural", DT::dataTableOutput("strData"))
                   
                  )
                )
              )
            )
          ),
               
      
    
      tabItem("anls1",
              
              fluidPage(
                h1("Analysis - Correlation Plot"),
                br(),
                box(plotOutput("correlation_plot"), width = 8),
                
                box(
                  selectInput("features", "Features:", c("pRatio","tsoRatio")),width = 4
                )
              )),
      
      tabItem("trn",
              
              fluidPage(
                h1("Transactions"),
                br(),
                helpText("Click the column header to sort a column."),
                dataTableOutput("trnData"))),
      
      
      tabItem("anls",
              
              fluidPage(
                h1("Analysis"),
                br(),
                helpText("Click the column header to sort a column."),
                dataTableOutput("anlsData"))),
      
      tabItem("pvt",
              
              fluidPage(
                h1("Pivot Tool"),
                br(),
                sidebarLayout(
                  
                  sidebarPanel(
                    fileInput("file", "Upload a * .csv file with headers")),
                  
                  # Show a plot of the generated distribution
                  mainPanel( rpivotTableOutput("dt")))
            )
          ) 
        )
      )
    )

# Server functions and callbacks

server <- function(input, output) {
  
  #Callback for search bar
  
  #==============================================================================================
  
  
  output$res <- renderPrint({
    
    #input$search
    
    shell.exec(input$search)
  })
  
  
  #==============================================================================================
  
  #Callback for dynamic table to call structural changes dataset and show selected columns only
  
  structural2 = structural[sample(nrow(structural), 1000), ]
  
  output$strData <- DT::renderDataTable({
    DT::datatable(structural2[, input$show_vars, drop = FALSE])
  })
  
  #==============================================================================================
  
  #Callback function for histogram containing structural dataset
  
  output$histogram <- renderPlot({hist(structural$pbin, breaks = input$bins, col=input$bincolor, border = 'grey30')  })
  
  #==============================================================================================
  
  #Callback to insert download as per selected file extension
  
  output$trnData <- DT::renderDataTable(transactions, extensions='Buttons', options= list(dom='Bfrtip', buttons = list('copy','pdf','csv','excel','print')))
  
  #==============================================================================================
  
  #Callback to render analysis dataset
  
  output$anlsData<- DT::renderDataTable(analysis)
  
  #==============================================================================================
  
  #Callback to render correlation plot in analysis data
  
  output$correlation_plot <- renderPlot({plot(analysis$PBIN, analysis[[input$features]],xlab = "PBIN", ylab = "Feature") })
  
  #Callback for pivot tool
  output$dt <- renderRpivotTable({
    file1= input$file
    if(is.null(file1)) { return()}
    data = read.table(file= file1$datapath, sep= ",", header = TRUE, fill = TRUE)
    if(is.null(data())){return()}
    rpivotTable(data)
  })
  #==============================================================================================
  
  #Callback to render histogram plot with dinamic colors
  
  output$elementPlot <- renderPlot({
    column = input$column
    plot(transactions$transactionID, transactions$SUPPLIER_NB, col=ifelse(transactions$COLUMN_NM==column, "red","grey"),
         main = "Supplier Number and Transaction ID per Element", xlab = "Transaction ID", ylab = "Supplier Number",log="xy")
    options(scipen=999)
  })
  
  #==============================================================================================
  
  #Callback for value box values calculating based on count rows
  
  output$trans = renderValueBox({
    valueBox(
      count(transactions), "Transaction Records", icon = icon("book"),
      color = "olive"
    )
  }) #ending of valuebox
  
  #==============================================================================================
  
  output$analys = renderValueBox({
    valueBox(
      count(analysis), "Analysis Records", icon = icon("pie-chart"),
      color = "olive"
    )
  }) #ending of valuebox
  
  #==============================================================================================
  
  output$struc = renderValueBox({
    valueBox(
      sum(abs(transactions$strChange)), "Structural Changes in Transactions", icon = icon("cubes"),
      color = "olive"
    )
  }) #ending of valuebox
  
  #==============================================================================================
  
  
  #Function to format % values in value box structural change  --change dataset accordingly
  
  myVar= 100*(sum(abs(transactions$strChange))/count(transactions))
  val= paste0(sprintf("%2.f", myVar), "%")
  
  #Value box for structural percentage-- it takes the formated % value from above function
 
  output$trnPercent = renderInfoBox({
    infoBox("Structural Changes %",
      val, icon = icon("percentage"),
      color = "orange"
    )
  }) #end
  
  #==============================================================================================
  
  #Getting values from elements in Analysis dataset VECTOR
  
  
  v_Name <- sum(row_count(analysis['COLUMN_NM'], count="Name", append=FALSE))
  v_Emp <-sum(row_count(analysis['COLUMN_NM'], count="Emp_Size", append=FALSE))
  v_Sls <-sum(row_count(analysis['COLUMN_NM'], count="Sls_Size", append=FALSE))
  v_AIN <- sum(row_count(analysis['COLUMN_NM'], count="AIN", append=FALSE))
  v_PhoneA <- sum(row_count(analysis['COLUMN_NM'], count="Phone_AREA", append=FALSE))
  v_PhonePFX <- sum(row_count(analysis['COLUMN_NM'], count="Phone_PFX", append=FALSE))
  v_PhoneSFX <- sum(row_count(analysis['COLUMN_NM'], count="Phone_SFX", append=FALSE))
  v_SIC <- sum(row_count(analysis['COLUMN_NM'], count="SIC", append=FALSE))
  
  #Assigning variables to subset of data 
  
  dat <- c(rep("Name", v_Name),
            rep("Emp Size", v_Emp),
            rep("Sales Size", v_Sls),
            rep("AIN", v_AIN),
            rep("Phone Area", v_PhoneA),
            rep("Phone PFX", v_PhonePFX),
            rep("Phone SFX", v_PhoneSFX),
            rep("SIC", v_SIC))
  
  # Call functions from ECharts2Shiny to render charts with the subset data

  renderPieChart(div_id = "test", data = dat)
  
}

#==============================================================================================

shinyApp(ui = ui, server = server)
