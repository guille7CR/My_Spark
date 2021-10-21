#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#



# Load packages
library(shiny)
library(shinysurveys)
library(googledrive)
library(googlesheets4)
library(tidyr)



options(
    # whenever there is one account token found, use the cached token
    gargle_oauth_email = TRUE,
    # specify auth tokens should be stored in a hidden directory ".secrets"
    gargle_oauth_cache = "Google_Shiny_App/.secrets"
)

# Get the ID of the sheet for writing programmatically
# This should be placed at the top of your shiny app
sheet_id <- drive_get("Survey_Sheet")$id


# Define questions in the format of a shinysurvey
one <- data.frame(
    question = "What's your name?",
    option = 'Your Name',
    input_type = "text",
    input_id = "name",
    dependence = NA,
    dependence_value = NA,
    required = TRUE
)

two <- data.frame(
    question = "What is your favorite food?",
    option = "Type",
    input_type = "text",
    input_id = "food",
    dependence = NA,
    dependence_value = NA,
    required = TRUE
)

three <- data.frame(
    question = "What is your favorite hobbie?",
    option = "Type",
    input_type = "text",
    input_id = "hobbie",
    dependence = NA,
    dependence_value = NA,
    required = TRUE
)

four <- data.frame(
    question = "Your Age",
    option = "Type",
    input_type = "numeric",
    input_id = "age",
    dependence = NA,
    dependence_value = NA,
    required = TRUE
)

five <- data.frame(
    question = "Province",
    option = c("San Jose", "Alajuela", "Heredia","Cartago","Guanacaste","Puntarenas","Limon"),
    input_type = "select",
    input_id = "province",
    dependence = NA,
    dependence_value = NA,
    required = TRUE
)


six<- data.frame(
    question = "Gender",
    option = c("M", "F", "Other"),
    input_type = "mc",
    input_id = "gender",
    dependence = NA,
    dependence_value = NA,
    required = TRUE
)

df=rbind(one,two,three,four,five, six)

df<- as_tibble(df)

# Define shiny UI
ui <-fluidPage(
    
   
    
    surveyOutput(df,
                 survey_title = "My Shiny Survey!",
                 survey_description = "A sample shiny survey")
)

# Define shiny server
server <- function(input, output, session) {
    
    
    renderSurvey()
    
    observeEvent(input$submit, {
        response_data <- getSurveyData()
        
        
        # Read our sheet
        values <- read_sheet(ss = sheet_id, 
                             sheet = "main")
        
        # Check to see if our sheet has any existing data.
        # If not, let's write to it and set up column names. 
        # Otherwise, let's append to it.
        
        if (nrow(values) == 0) {
            sheet_write(data = response_data,
                        ss = sheet_id,
                        sheet = "main")
        } else {
            sheet_append(data = response_data,
                         ss = sheet_id,
                         sheet = "main")
        }
        
    })
    
    observeEvent(input$submit, {
        showModal(modalDialog(
            title = "Congrats, you completed your shinysurvey!",
            "You can now close this page. Thanks."
        ))
    }) 
    
    
}
# Run the application 
shinyApp(ui = ui, server = server)
