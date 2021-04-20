
bestfleet_optimizer_Rsymphony <- function(route_costs,lp_optimization_matrix){
  
#################################################
#Define model - Objective function & Constraints
#################################################

#costs
input_cost <- route_costs

input_cost$ID <- 1

input_cost <- input_cost %>% mutate(sub_segment_ref = paste0(input_cost$DSC_ROUTE,"_",input_cost$ATR_ICAO_COD))

ALL_VARIABLES <- lp_optimization_matrix %>% select(-CONSTRAINT_TYPE,-CONSTRAINT_VALUE, -VARIABLE_DIRECTION, -VARIABLE_VALUE)

ALL_VARIABLES <- as.data.frame(colnames(ALL_VARIABLES))

names(ALL_VARIABLES)[names(ALL_VARIABLES) == "colnames(ALL_VARIABLES)"] <- "segment_ref"

sub_lenght <- str_length((ALL_VARIABLES$segment_ref)[1])
sub_component_1 <- substr(ALL_VARIABLES$segment_ref, 1, 10)
sub_component_2 <- substr(ALL_VARIABLES$segment_ref, sub_lenght-3, sub_lenght)
ALL_VARIABLES <- ALL_VARIABLES %>% mutate(sub_segment_ref = paste0(sub_component_1,"_",sub_component_2))

input_cost <- inner_join(input_cost,ALL_VARIABLES)

list_ColumnsOrder <- as.vector(t(input_cost$segment_ref))

lp_optimization_matrix <- lp_optimization_matrix %>% select(CONSTRAINT_TYPE,CONSTRAINT_VALUE, VARIABLE_DIRECTION, VARIABLE_VALUE, list_ColumnsOrder)


#Objective function
objective_function <- as.vector(t(input_cost$TOTAL_COST))

objective.in = objective_function


#Constraints Matrix
constraints_matrix <- lp_optimization_matrix %>% select(-CONSTRAINT_TYPE,-CONSTRAINT_VALUE, -VARIABLE_DIRECTION, -VARIABLE_VALUE)

const.mat <- as.matrix(constraints_matrix)

colnames(const.mat) <- NULL

#Constraints Values
#RHS for the Constraints
const.rhs <- lp_optimization_matrix$VARIABLE_VALUE


#Constraints directions
const.dir <- lp_optimization_matrix$VARIABLE_DIRECTION

#Compute Optimal Solution

start <- Sys.time()

optimal_solution_Rsymphony <- Rsymphony_solve_LP(objective.in, const.mat, const.dir, const.rhs, bounds = NULL, types = "B",
                   max = FALSE, verbosity = -2, time_limit = -1,
                   node_limit = -1, gap_limit = -1, first_feasible = FALSE,
                   write_lp = FALSE, write_mps = FALSE)

end <- Sys.time()

run_time <- end - start

return(list(optimal_solution_Rsymphony,list_ColumnsOrder))

}
