#Clear GLobal Environment
rm(list=ls())

##########
#Notation
##########
options(scipen=999)

##############################
#Period of analysis - for SPs
##############################
start_date <- 20210108
end_date <- 20210131


###########
#Libraries
###########

#Data manipulation
library(dplyr)
library(stringr)
library(data.table)
library(DescTools)

#Modelation
library(Rsymphony)


###############
#Set directory
###############

wd <- "//tapnet.tap.pt/bi/Dev/DW/ANALYTICS/R Projects/20210119_Best_Fleet_For_Route/"
#wd <- "E:/ANALYTICS/PROJECTS/20210119_Best_Fleet_For_Route/Code"
setwd(wd)


###########
#Functions
###########

# Set R functions path ----------------------------------------------------

R_functions_path <- paste0(wd, "/../R_Functions")
file.sources <- list.files(R_functions_path)
#file.sources <- file.sources[file.sources %like% c('%.R')]
sapply(paste0(R_functions_path, "/", file.sources), source, encoding = "UTF-8",.GlobalEnv)
rm(R_functions_path, file.sources)

#Load functions

#Matrices manipulation
sapply(paste0(wd,"/03_Functions/f_unique_matrix.R"), source, encoding = "UTF-8",.GlobalEnv)

#Audit
sapply(paste0(wd,"/03_Functions/f_audit_matrix.R"), source, encoding = "UTF-8",.GlobalEnv)

#Matrices
sapply(paste0(wd,"/03_Functions/f_set_matrices.R"), source, encoding = "UTF-8",.GlobalEnv)

#SPs
sapply(paste0(wd,"/03_Functions/f_inputs_SPs.R"), source, encoding = "UTF-8",.GlobalEnv)

#Optimizer
sapply(paste0(wd,"/02_R/fleet_optimization_Rsymphony.R"), source, encoding = "UTF-8",.GlobalEnv)




##############
#Aux variable
##############

#f_unique_matrix join options
aux_join <- c("CONSTRAINT_TYPE", "VARIABLE_NAME", "CONSTRAINT_VALUE")

aux_join_1 <- c("VARIABLE_NAME")


#Matrices columns order - Matrix Constraints & Matrix Constraints Values
matrix_constraints_columns_order <- c("TYPE_CONSTRAINT_VALUE", "CONSTRAINT_TYPE","CONSTRAINT_VALUE","VARIABLE_NAME","VARIABLE_COMPONENT","VARIABLE_COMPONENT_VALUE")

matrix_constraints_values_columns_order <- c("CONSTRAINT_TYPE","CONSTRAINT_VALUE","VARIABLE_NAME","VARIABLE_VALUE","VARIABLE_DIRECTION")

########
#Inputs
########

#SPs function
inputs_SPs(start_date,end_date)

#Aux variables - SQL
sql_where <- " WHERE 1=1"

#Operational costs
sql_costs <- paste0("SELECT DSC_ROUTE, ATR_ICAO_COD, SUM(TOTAL_COST) AS TOTAL_COST FROM [DW_ANALYTICS].[INPUT].[BF4R_INPUT_COSTS] GROUP BY DSC_ROUTE, ATR_ICAO_COD")

route_costs <- DBI_load_dwh_dataset('PRD', sql_costs)

route_costs <- route_costs %>% select(1,2,TOTAL_COST)


#Bookings
sql_bookings <- paste0("SELECT * FROM [DW_ANALYTICS].[INPUT].[BF4R_INPUT_BOOKINGS] ", sql_where, " order by DSC_ROUTE")

bookings <- DBI_load_dwh_dataset('PRD', sql_bookings)


#Number of flights
sql_nb_flights <- paste0("SELECT DISTINCT * FROM [DW_ANALYTICS].[INPUT].[BF4R_INPUT_SCOPE_BY_ROUTE]", gsub("DSC_ROUTE","DEP_DSC_ROUTE", sql_where) )

nb_flights <- DBI_load_dwh_dataset('PRD', sql_nb_flights)


#Fleet cacapity
sql_fleetcapacity <- "SELECT * FROM [DW_ANALYTICS].[INPUT].[BF4R_REF_DATA_03_CAPACITIES]"

bookings_fleetcapacity <- DBI_load_dwh_dataset('PRD', sql_fleetcapacity)


#Fleet availability
sql_fleet_availability <- "SELECT * FROM [DW_ANALYTICS].[INPUT].[BF4R_INPUT_AVAILABLE_TAILS_BY_FLEET_HR]"

fleet_availability <- DBI_load_dwh_dataset('PRD', sql_fleet_availability)


#Costs - Detailed
sql_detailed_costs <- paste0("SELECT * FROM [DW_ANALYTICS].[INPUT].[BF4R_INPUT_COSTS]")

route_detailed_costs <- DBI_load_dwh_dataset('PRD', sql_detailed_costs)

#Cost by Route
sql_route_total_costs <- paste0("  SELECT DSC_ROUTE, 
  ATR_ICAO_COD, 
  sum(CREW_COST) as ROUTE_CREW_COST_OPTIM,
  sum(FUEL_COST) as ROUTE_FUEL_COST_OPTIM,
  sum(AIRPORT_COST) as ROUTE_AIRPORT_COST_OPTIM, 
  SUM(TOTAL_COST) AS ROUTE_TOTAL_COST_OPTIM
  FROM [DW_ANALYTICS].[OPTIM].[BF4R_INPUT_COSTS]
  --WHERE DSC_ROUTE = 'LIS-GIG/VV'
  GROUP BY DSC_ROUTE, ATR_ICAO_COD")

route_total_costs <- DBI_load_dwh_dataset('PRD', sql_route_total_costs)

#Tails
sql_tails <- paste0("SELECT distinct
      [ATR_ICAO_COD],
      [COD_FLEET],
      [COD_SUB_FLEET]
  FROM [DW_ANALYTICS].[OPTIM].[BF4R_REF_DATA_01_TAIL_NUMBERS]")

fleet_tails <- DBI_load_dwh_dataset('PRD', sql_tails)

#Output
sql_output <- paste0("SELECT * FROM DW_ANALYTICS.OUTPUT.BF4R_OUTPUT_SCOPE_BY_ROUTE", sql_where)

lp_opt_output <- DBI_load_dwh_dataset('PRD', sql_output)

order_output <- colnames(lp_opt_output)

################
#Model Matrices
################

#Call function
aux_matrices <-set_matrices()

matrix_constraints <- as.data.frame(aux_matrices[1])

matrix_constraints_values<- as.data.frame(aux_matrices[2])


##################
#Global variables
##################

ORIGIN = "LIS"
TYPE_CONSTRAINT_VALUE <- ""
CONSTRAINT_TYPE <- ""
CONSTRAINT_VALUE <- ""
VARIABLE_NAME <- ""
VARIABLE_VALUE <- ""
VARIABLE_DIRECTION <- ">="
VARIABLE_COMPONENT <- ""
VARIABLE_COMPONENT_VALUE <- ""

all_variables <- bookings %>% distinct(VARIABLE_NAME = DSC_ROUTE_KEY)
all_variables_1 <- route_costs %>% distinct(VARIABLE_NAME = DSC_ROUTE)
all_variables_2 <- bookings %>% distinct(VARIABLE_NAME = DSC_ROUTE_KEY, SCHD_DEP_LOC_HR)


#############
#Build model
#############

#Bookings
#Adjust global variables
TYPE_CONSTRAINT_VALUE <- "GLOBAL"
CONSTRAINT_TYPE <- "BOOKINGS"
CONSTRAINT_VALUE <- c("OUTBOUND", "INBOUND")

#Matrix Constraints
bookings_fleetcapacity <- merge(all_variables, bookings_fleetcapacity) %>% select(VARIABLE_NAME,VARIABLE_COMPONENT = ATR_ICAO_COD, VARIABLE_COMPONENT_VALUE = AVG_TOTAL_CAPACITY)

bookings_fleetcapacity <- bookings_fleetcapacity %>% mutate(TYPE_CONSTRAINT_VALUE,CONSTRAINT_TYPE)

bookings_fleetcapacity <- merge(bookings_fleetcapacity, as.data.frame(CONSTRAINT_VALUE))

bookings_fleetcapacity <- bookings_fleetcapacity %>% mutate(CONSTRAINT_VALUE = paste0(CONSTRAINT_VALUE, "_",VARIABLE_NAME))
  
bookings_fleetcapacity <- bookings_fleetcapacity %>% select(matrix_constraints_columns_order)

#Matrix Constraints Values
  
bookings_out_in <- bookings %>% group_by(SCHD_DEP_LOC, DSC_ROUTE_KEY, CONSTRAINT_VALUE = DSC_DIRECTION) %>% 
  summarise(VARIABLE_VALUE = sum(TOTAL_BOOKED)) %>% 
  mutate(CONSTRAINT_TYPE = CONSTRAINT_TYPE, VARIABLE_NAME = DSC_ROUTE_KEY)

bookings_out_in$CONSTRAINT_VALUE <- str_replace(bookings_out_in$CONSTRAINT_VALUE,"IDA", CONSTRAINT_VALUE[1])
bookings_out_in$CONSTRAINT_VALUE <- str_replace(bookings_out_in$CONSTRAINT_VALUE,"VOLTA", CONSTRAINT_VALUE[2])

bookings_out_in$CONSTRAINT_VALUE <- paste0(bookings_out_in$CONSTRAINT_VALUE,"_",bookings_out_in$VARIABLE_NAME)

bookings_out_in$VARIABLE_DIRECTION <- VARIABLE_DIRECTION

bookings_out_in <- bookings_out_in %>% ungroup()
bookings_out_in <- bookings_out_in %>% select(CONSTRAINT_TYPE, CONSTRAINT_VALUE,VARIABLE_NAME,VARIABLE_VALUE,VARIABLE_DIRECTION)

bookings_matrix_values <- bookings_out_in

#Add data for model 
matrix_constraints <- rbind(matrix_constraints,bookings_fleetcapacity)
matrix_constraints$CONSTRAINT_VALUE <- as.character(matrix_constraints$CONSTRAINT_VALUE)
matrix_constraints_values <- rbind(matrix_constraints_values,bookings_matrix_values)

lp_optimization_matrix <- unique_matrix(matrix_constraints,matrix_constraints_values, aux_join)


#Call audit
matrix_status <- audit_matrix(lp_optimization_matrix,matrix_2, all_variables, all_variables_2, bookings_fleetcapacity$VARIABLE_COMPONENT, aux_join, 4, 0)


#Reset matrices
matrix_constraints <- as.data.frame(aux_matrices[1])

matrix_constraints_values<- as.data.frame(aux_matrices[2])


#Fleets availability
fleet_availability <- DBI_load_dwh_dataset('PRD', sql_fleet_availability)

#Global variables
TYPE_CONSTRAINT_VALUE <- "INDIVIDUAL"
CONSTRAINT_TYPE <- "MAXIMUM_AIRPLANES"
#CONSTRAINT_VALUE <- all_fleets
VARIABLE_NAME <- all_variables_2
VARIABLE_VALUE <- ""
VARIABLE_DIRECTION <- "<="

VARIABLE_COMPONENT <- ""
VARIABLE_COMPONENT_VALUE <- 1

all_fleets <- fleet_availability %>% distinct(VARIABLE_COMPONENT = ATR_ICAO_COD)

fleet_availability<- fleet_availability %>% group_by(VARIABLE_COMPONENT = ATR_ICAO_COD, SCHD_DEP_DT_HR) %>% summarise(VARIABLE_VALUE = sum(TOTAL_TAIL))

fleet_availability <- left_join(fleet_availability, all_variables_2, by = c("SCHD_DEP_DT_HR" = "SCHD_DEP_LOC_HR"))

fleet_availability <- fleet_availability %>% filter(!is.na(VARIABLE_NAME))

fleet_availability$CONSTRAINT_VALUE <- paste0(fleet_availability$VARIABLE_COMPONENT,"_",fleet_availability$VARIABLE_NAME)


#Built Matrix Constraints
#Data

#TYPE_CONSTRAINT_VALUE
fleet_availability_maximum <- fleet_availability %>% mutate(TYPE_CONSTRAINT_VALUE,CONSTRAINT_TYPE,CONSTRAINT_VALUE,VARIABLE_COMPONENT_VALUE) %>% select(matrix_constraints_columns_order)

fleet_availability_maximum <- fleet_availability_maximum %>% distinct(TYPE_CONSTRAINT_VALUE,CONSTRAINT_TYPE,CONSTRAINT_VALUE,VARIABLE_NAME,VARIABLE_COMPONENT,VARIABLE_COMPONENT_VALUE)

fleet_availability_maximum <- data.frame(fleet_availability_maximum) 

#Built Matrix Constraints Values
#Data
fleet_availability <- fleet_availability %>% ungroup()

fleet_availability_maximum_values <- fleet_availability %>% mutate(CONSTRAINT_TYPE, CONSTRAINT_VALUE, VARIABLE_DIRECTION) %>% select(matrix_constraints_values_columns_order)

fleet_availability_maximum_values <- fleet_availability_maximum_values %>% group_by(CONSTRAINT_TYPE, CONSTRAINT_VALUE, VARIABLE_NAME,VARIABLE_DIRECTION) %>% 
                                                                           summarize(VARIABLE_VALUE = sum(VARIABLE_VALUE)) %>% 
                                                                           select(matrix_constraints_values_columns_order)
fleet_availability_maximum_values <- data.frame(fleet_availability_maximum_values)

#Add data for model 
matrix_constraints <- rbind(matrix_constraints,fleet_availability_maximum)

matrix_constraints_values <- rbind(matrix_constraints_values,fleet_availability_maximum_values)

#Add to final matrix
lp_optimization_matrix_aux <- unique_matrix(matrix_constraints,matrix_constraints_values, aux_join)
lp_optimization_matrix <- rbind(lp_optimization_matrix,lp_optimization_matrix_aux)

#Call audit - Fleets
matrix_status <- audit_matrix(lp_optimization_matrix_aux, matrix_2, all_variables, all_variables_2, bookings_fleetcapacity$VARIABLE_COMPONENT, aux_join, 4, 0)


#Reset matrices
matrix_constraints <- as.data.frame(aux_matrices[1])

matrix_constraints_values<- as.data.frame(aux_matrices[2])


#Number of Flights
#Global variables
TYPE_CONSTRAINT_VALUE <- "GLOBAL"
CONSTRAINT_TYPE <- "NUMBER_FLIGHTS"
CONSTRAINT_VALUE <- "MINIMUM_FLIGHTS"
VARIABLE_NAME <- all_variables
VARIABLE_VALUE <- ""
VARIABLE_DIRECTION <- ">="

VARIABLE_COMPONENT <- all_fleets
VARIABLE_COMPONENT_VALUE <- 1

nb_flights$SCHD_DEP_TM_LOC <- str_replace_all(nb_flights$SCHD_DEP_TM_LOC, "[[:punct:]]", "")

nb_flights <- nb_flights %>% mutate(VARIABLE_NAME = paste0(DEP_DSC_ROUTE,"_",ID_SCHD_DEP_DT_LOC,"_", SCHD_DEP_TM_LOC))

nb_flights <- nb_flights %>% group_by(DEP_DSC_ROUTE,VARIABLE_NAME) %>% mutate(VARIABLE_VALUE = n())

nb_flights$DEP_DSC_ROUTE <- NULL

#Built Matrix Constraints
#Data

#TYPE_CONSTRAINT_VALUE

nb_flights_matrix <- merge(nb_flights, all_fleets)

nb_flights_matrix <- nb_flights_matrix %>% mutate(TYPE_CONSTRAINT_VALUE,CONSTRAINT_TYPE,CONSTRAINT_VALUE=paste0(CONSTRAINT_VALUE,"_",VARIABLE_NAME),VARIABLE_NAME,VARIABLE_COMPONENT_VALUE) %>% select(matrix_constraints_columns_order)

#Built Matrix Constraints Values
#Data
nb_flights_matrix_values <- merge(nb_flights, all_fleets)

nb_flights_matrix_values <- nb_flights_matrix_values %>% mutate(CONSTRAINT_TYPE,CONSTRAINT_VALUE=paste0(VARIABLE_COMPONENT,"_",VARIABLE_NAME), VARIABLE_DIRECTION,VARIABLE_VALUE ) %>% select(matrix_constraints_values_columns_order)

#Add data for model
matrix_constraints <- nb_flights_matrix

matrix_constraints_values <- nb_flights_matrix_values %>% select(VARIABLE_NAME, VARIABLE_VALUE, VARIABLE_DIRECTION)

lp_optimization_matrix_0 <- matrix_constraints %>% left_join(matrix_constraints_values,by = as.character(aux_join_1))

lp_optimization_matrix_1 <- lp_optimization_matrix_0 %>% select(CONSTRAINT_TYPE, CONSTRAINT_VALUE, VARIABLE_NAME,VARIABLE_COMPONENT, VARIABLE_COMPONENT_VALUE, VARIABLE_VALUE, VARIABLE_DIRECTION)

lp_optimization_matrix_2 <- dcast(setDT(lp_optimization_matrix_1), CONSTRAINT_TYPE + CONSTRAINT_VALUE + VARIABLE_DIRECTION + VARIABLE_VALUE ~ VARIABLE_NAME + VARIABLE_COMPONENT, value.var = c("VARIABLE_COMPONENT_VALUE"))

lp_optimization_matrix_2[is.na(lp_optimization_matrix_2)] <- 0

lp_optimization_matrix_2[lp_optimization_matrix_2 == 10] <- 1

lp_optimization_matrix_aux <-lp_optimization_matrix_2

#Add to final matrix

lp_optimization_matrix <- rbind(lp_optimization_matrix, lp_optimization_matrix_aux)

#Call audit - Number of Flights
matrix_status <- audit_matrix(lp_optimization_matrix_aux, matrix_2, all_variables, lp_optimization_matrix_2, bookings_fleetcapacity$VARIABLE_COMPONENT, aux_join, 4, 0)
matrix_status

# var1 <- data.frame(colnames(lp_optimization_matrix))
# var2 <- data.frame(colnames(lp_optimization_matrix_aux))
# 
# colnames(var1) <- "col"
# colnames(var2) <- "col"
# 
# diff <- anti_join(var2,var1)
# diff1 <- anti_join(var1,var2)


################
#Call optimizer
################

#Call function
run_time <- Sys.time()

start_time <- Sys.time()

optimal_solution_list <- bestfleet_optimizer_Rsymphony(route_costs,lp_optimization_matrix)

optimal_solution <- optimal_solution_list[1]

end_time <- Sys.time()

end_time-start_time


#Output results
destination <- as.data.frame(optimal_solution_list[2])

solution <- as.data.frame(optimal_solution[[1]]$solution)

optimal_solution <- cbind(destination, solution)

colnames(optimal_solution) <- c("Route_Fleet", "Solution")


#Write table
optimal_solution_t <- data.frame(str_split_fixed(optimal_solution$Route_Fleet, "_", 4))

optimal_solution_aux <- optimal_solution %>% cbind(optimal_solution_t)

optimal_solution_aux <- optimal_solution_aux %>% cbind(run_time)

optimal_solution_aux$X5 <- paste0(optimal_solution_aux$X2, optimal_solution_aux$X3)

optimal_solution_aux <- optimal_solution_aux %>% select(run_time, DEP_DSC_ROUTE = X1, SCHD_DEP_LOC = X5, OPT_FLEET = X4, Solution )

optimal_solution_aux$DEP_DSC_ROUTE <- as.character(optimal_solution_aux$DEP_DSC_ROUTE)

optimal_solution_aux$OPT_FLEET <- as.character(optimal_solution_aux$OPT_FLEET)

optimal_solution_aux <- optimal_solution_aux %>% filter(solution == 1)

optimal_solution_aux_total <- optimal_solution_aux %>% group_by(DEP_DSC_ROUTE,SCHD_DEP_LOC) %>% summarise(total = n())

#Call audit - Bookings
matrix_status <- audit_matrix(lp_optimization_matrix,matrix_2, all_variables, all_variables_2, bookings_fleetcapacity$VARIABLE_COMPONENT, aux_join, 4,0)

#Call audit - Output
matrix_status <- audit_matrix(lp_opt_output,optimal_solution_aux, all_variables, all_variables_2, bookings_fleetcapacity$VARIABLE_COMPONENT, aux_join, 4,1)


#Original Costs
original_decision <- lp_opt_output

original_decision <- original_decision[,colSums(is.na(original_decision))<nrow(original_decision)]

route_detailed_costs$IS_DEP_APT <- ifelse(substr(route_detailed_costs$DSC_ROUTE,1,3) == route_detailed_costs$COD_DEP_APT,1,0)

OUTBOUND_COSTS <- route_detailed_costs %>% filter(IS_DEP_APT == 1) %>% 
  select(DSC_ROUTE,ATR_ICAO_COD,COD_FLEET, CREW_COST_OPTIM = CREW_COST, FUEL_COST_OPTIM = FUEL_COST, AIRPORT_COST_OPTIM = AIRPORT_COST, TOTAL_COST_OPTIM = TOTAL_COST)

INBOUND_COSTS <- route_detailed_costs %>% filter(IS_DEP_APT == 0) %>% 
  select(DSC_ROUTE,ATR_ICAO_COD,COD_FLEET, NEXT_FLIGHT_CREW_COST_OPTIM = CREW_COST, NEXT_FLIGHT_FUEL_COST_OPTIM = FUEL_COST, NEXT_FLIGHT_AIRPORT_COST_OPTIM = AIRPORT_COST, NEXT_FLIGHT_TOTAL_COST_OPTIM = TOTAL_COST)

OPTM_COST <- OUTBOUND_COSTS %>% left_join(INBOUND_COSTS) %>% left_join(route_total_costs)

optimal_decision <- optimal_solution_aux %>% group_by(DEP_DSC_ROUTE,SCHD_DEP_LOC,OPT_FLEET) %>% summarise(total = n())

optimal_decision <- optimal_solution_aux %>% left_join(route_costs, by = c("DEP_DSC_ROUTE" = "DSC_ROUTE","OPT_FLEET" ="ATR_ICAO_COD"))

optimal_decision <- optimal_decision %>% left_join(optimal_solution_aux_total, by = c("DEP_DSC_ROUTE" = "DEP_DSC_ROUTE","SCHD_DEP_LOC" ="SCHD_DEP_LOC"))

names(optimal_decision)[names(optimal_decision) == "OPT_FLEET"] <- "ATR_ICAO_COD_OPTIM"

names(optimal_decision)[names(optimal_decision) == "total"] <- "TOTAL_FLEETS_SELECTED"

names(optimal_decision)[names(optimal_decision) == "TOTAL_COST"] <- "TOTAL_COST_OPTIM_ALGO"

optimal_decision <- optimal_decision %>% left_join(OPTM_COST, by = c("DEP_DSC_ROUTE" = "DSC_ROUTE","ATR_ICAO_COD_OPTIM" ="ATR_ICAO_COD"))

optimal_decision <- optimal_decision %>% left_join(fleet_tails, by = c("ATR_ICAO_COD_OPTIM" ="ATR_ICAO_COD" , "COD_FLEET" = "COD_FLEET"))
                                      
optimal_decision <- optimal_decision %>% mutate(COD_FLEET_OPTIM = COD_FLEET, COD_SUB_FLEET_OPTIM = COD_SUB_FLEET)

optimal_decision <- optimal_decision %>% select(-COD_FLEET, -COD_SUB_FLEET)

#ADD OPTIMAL COST TO OUTPUT
original_decision$SCHD_DEP_LOC <- as.character(original_decision$SCHD_DEP_LOC)

lp_opt_output_aux <- original_decision %>% inner_join(optimal_decision, by = c("SCHD_DEP_LOC" = "SCHD_DEP_LOC", "DSC_ROUTE" = "DEP_DSC_ROUTE"))

lp_opt_output_aux <- lp_opt_output_aux %>% select(order_output)

# #OUTPUT to SQL
DBI_execute_dwh_query('PRD', 'TRUNCATE TABLE OUTPUT.BF4R_OUTPUT_SCOPE_BY_ROUTE')
DBI_save_dwh_dataset('PRD', 'OUTPUT.BF4R_OUTPUT_SCOPE_BY_ROUTE', lp_opt_output_aux)



