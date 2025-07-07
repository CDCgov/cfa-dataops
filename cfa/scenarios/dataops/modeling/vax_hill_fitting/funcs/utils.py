import pandas as pd
import numpy as np
import os
import json
from scipy.optimize import minimize
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

import warnings
warnings.filterwarnings("ignore")

#definte age levels and start dates for each
age_factor_levels = ['0-17', '18-49', '50-64', '65+']
start_date = ['2024-09-29', '2024-09-01', '2024-09-01', '2024-09-01']

vax_start_dates = pd.DataFrame({
    "age": age_factor_levels,
    "start_date": start_date,
}
)

#function for converting state names without spaces, dashes, or periods to nameid and all lowercase
def state_name_to_nameid(state_name):
    return state_name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")

#create state name to abbreviation mapping
state_name_abbrev_map = {
    "AL":"Alabama",
    "AK":"Alaska",
    "AZ":"Arizona",
    "AR":"Arkansas",
    "CA":"California",
    "CO":"Colorado",
    "CT":"Connecticut",
    "DE":"Delaware",
    "FL":"Florida",
    "GA":"Georgia",
    "HI":"Hawaii",
    "ID":"Idaho",
    "IL":"Illinois",
    "IN":"Indiana",
    "IA":"Iowa",
    "KS":"Kansas",
    "KY":"Kentucky",
    "LA":"Louisiana",
    "ME":"Maine",
    "MD":"Maryland",
    "MA":"Massachusetts",
    "MI":"Michigan",
    "MN":"Minnesota",
    "MS":"Mississippi",
    "MO":"Missouri",
    "MT":"Montana",
    "NE":"Nebraska",
    "NV":"Nevada",
    "NH":"New Hampshire",
    "NJ":"New Jersey",
    "NM":"New Mexico",
    "NY":"New York",
    "NC":"North Carolina",
    "ND":"North Dakota",
    "OH":"Ohio",
    "OK":"Oklahoma",
    "OR":"Oregon",
    "PA":"Pennsylvania",
    "RI":"Rhode Island",
    "SC":"South Carolina",
    "SD":"South Dakota",
    "TN":"Tennessee",
    "TX":"Texas",
    "UT":"Utah",
    "VT":"Vermont",
    "VA":"Virginia",
    "WA":"Washington",
    "WV":"West Virginia",
    "WI":"Wisconsin",
    "WY":"Wyoming"
}

state_nameids = [state_name_to_nameid(name) for name in state_name_abbrev_map.values()]

def state_abbrev_to_nameid(abbrev):
    return state_name_to_nameid(state_name_abbrev_map[abbrev])


def vax_scaledhill_with_endpoint(t, n, t_h, t_end, y_t_end):
    """
    Calculate vaccine uptake curve y(t) from t and parameters
    
    Parameters:
    t : pandas.Series or numpy.array
        time values
    n : float
        shape parameter
    t_h : float
        time of half saturation
    t_end : float
        time at which endpoint y-value is specified
    y_t_end : float
        endpoint y-value
    
    Returns:
    pandas.Series or numpy.array
        calculated y values (same type as input t)
    """
    # Derived by solving for the scaling coefficient when plugging in t_end, y_t_end
    ##had to implement try/except to prevent failing runs
    try:
        scale = (1 + (t_h / t_end)**n) * y_t_end
    except:
        scale = 1
    # Calculate the Hill function
    result = scale * t**n / (t**n + t_h**n)
    
    return result



def fit_vax_scaledhill_from_endpoint(t_obs, y_obs, t_end, y_t_end):
    """
    Fit Hill function given observations and anchored endpoint
    
    Parameters:
    t_obs : pandas.Series
        observation times
    y_obs : pandas.Series
        observed values
    t_end : float
        time at which endpoint y-value is specified
    y_t_end : float
        endpoint y-value
    
    Returns:
    dict
        Optimization result with fitted parameters
    """
    # Validate input constraint
    assert y_t_end <= 1.0, "y_t_end must be <= 1.0"
    # Convert to numpy arrays
    t_obs = np.array(t_obs)
    y_obs = np.array(y_obs)
    
    def vax_scaledhill_objective(par):
        """
        Objective function for optimization
        """
        n = par[0]      # shape parameter
        t_h = par[1]    # time of half saturation
        
        # Calculate scale factor
        try:
            scale = (1 + (t_h / t_end)**n) * y_t_end
        except:
            scale = 1.0
            
        # Check for invalid parameter values
        if not np.isfinite(scale) or scale <= 0.0 or n <= 0.0 or t_h <= 0.0:
            # print(f"bad value, scale: {scale:.6f}, n: {n:.6f}, t_h: {t_h:.6f}")
            return np.inf
        else:
            # Calculate predicted values using the Hill function
            y_pred = vax_scaledhill_with_endpoint(t_obs, n, t_h, t_end, y_t_end)
            # Return sum of squared errors
            return np.sum((y_pred - y_obs)**2)
    
    # Set initial parameter values
    # Choose n0 where asymptote is at 1 to ensure valid evaluation
    a0 = 0.99
    n0 = 1.0
    t_h_0 = (t_end**n0 * (a0 / y_t_end - 1))**(1/n0)  # solved from initial scale a0, shape n0
    # Alternative calculation (commented in original):
    # n0 = np.log(a0 / y_t_end - 1) / np.log(t_h_0 / t_end)  # Solved for n
    
    # Initial parameter vector
    initial_params = np.array([n0, t_h_0])
    
    # Perform optimization
    result = minimize(
        vax_scaledhill_objective,
        initial_params,
        method='Nelder-Mead',
        options={'maxiter': 1000}
    )
    
    # Return result in a format similar to R's optim
    return {
        'par': {'n': result.x[0], 't_h': result.x[1]},
        'value': result.fun,
        'convergence': 0 if result.success else 1,
        'message': result.message,
        'success': result.success
    }

def plot_timeseries(t_obs,
                    y_obs,
                    t_end,
                    y_t_end,
                    scenario_dir,
                    state_nameid,
                    age):
    """
    Plot time series data with different colors for different segments
    
    Parameters:
    t_obs : pandas.Series or numpy.array
        observation times
    y_obs : pandas.Series or numpy.array
        observed values
    t_end : float
        end time for plot range
    y_t_end : float
        end value for plot range
    """
    # Create color vector: first 10 points in black, rest in red
    n_obs = len(t_obs)
    colors = ['black'] * min(10, n_obs) + ['red'] * max(0, n_obs - 10)
    
    # Create the plot
    plt.figure(figsize=(8, 6))
    
    # Plot the observed data points with different colors
    plt.scatter(t_obs, y_obs, facecolors='none', edgecolors=colors, marker='o', s=50, alpha=0.7)

    # Plot the endpoint
    plt.scatter(t_end, y_t_end, c='black', marker='x', s=100, linewidth=2)

    # Add padding to plot limits (5% padding on each side)
    x_padding = t_end * 0.05  # 5% of x-range
    y_padding = y_t_end * 0.05  # 5% of y-range
    
    # Set plot limits with padding
    plt.xlim(-x_padding, t_end + x_padding)
    plt.ylim(-y_padding, y_t_end + y_padding)
    plt.xlabel('t_obs')
    plt.ylabel('y_obs')
    plt.title('Time Series Data')
    plt.grid(True, alpha=0.3)
    
    # Save plot
    plt.savefig(os.path.join(scenario_dir, f'{state_nameid}-{age}-timeseries.pdf'), 
                bbox_inches='tight', dpi=300)
    plt.close()

def plot_fit(
    t_obs,
    y_obs,
    t_end,
    y_t_end,
    fit_result,
    t_obs_for_fit=None,
    y_obs_for_fit=None,
    y_prev=None,
    scenario_dir=None,
    state_nameid=None,
    age=None
):
    """
    Plot fitted Hill curve with observed data and previous year's data
    
    Parameters:
    t_obs : pandas.Series or numpy.array
        observation times
    y_obs : pandas.Series or numpy.array
        observed values
    t_end : float
        end time for plot range
    y_t_end : float
        end value for plot range
    fit_result : dict
        optimization result with fitted parameters
    t_obs_for_fit : pandas.Series or numpy.array, optional
        observation times used for fitting (default: t_obs)
    y_obs_for_fit : pandas.Series or numpy.array, optional
        observed values used for fitting (default: y_obs)
    y_prev : pandas.Series or numpy.array, optional
        previous year's vaccination data
    """
    # Set default values for optional parameters
    if t_obs_for_fit is None:
        t_obs_for_fit = t_obs
    if y_obs_for_fit is None:
        y_obs_for_fit = y_obs
    
    # Create color vector: first 10 points in black, rest in red
    n_obs = len(t_obs)
    colors = ['black'] * min(10, n_obs) + ['red'] * max(0, n_obs - 10)
    
    # Create the plot
    plt.figure(figsize=(10, 8))
    
    # Plot the observed data points with different colors
    plt.scatter(t_obs, y_obs, facecolors='none', color=colors, marker='o', s=50, alpha=0.7, 
               label='Observed Data')
    
    # Extract fitted parameters
    n = fit_result['par']['n']
    t_h = fit_result['par']['t_h']
    
    # Generate prediction points for smooth curve
    t_pred = np.arange(0, int(t_end) + 1)
    y_pred = vax_scaledhill_with_endpoint(t_pred, n, t_h, t_end, y_t_end)
    
    # Plot the fitted curve
    plt.plot(t_pred, y_pred, 'k-', linewidth=2, label='Fitted Curve')
    
    # Plot previous year's data if provided
    if y_prev is not None:
        plt.plot(t_obs, y_prev, 'b--', linewidth=2, label='23-24 Vaccinations')
    
    # Add padding to plot limits (5% padding on each side)
    x_padding = t_end * 0.05  # 5% of x-range
    y_padding = y_t_end * 0.05  # 5% of y-range
    
    # Set plot limits with padding
    plt.xlim(-x_padding, t_end + x_padding)
    plt.ylim(-y_padding, y_t_end + y_padding)
    plt.xlabel('t_obs')
    plt.ylabel('y_obs')
    plt.title('Hill Function Fit to Vaccination Data')
    plt.grid(True, alpha=0.3)
    
    # Add legends
    # Main legend for lines
    plt.legend(loc='upper left', bbox_to_anchor=(0.02, 0.98))

    # Create custom legend for data points

    legend_elements = [
        Line2D([0], [0], color='black', linewidth=2, label='Fitted Curve'),
    ]
    legend_elements.append(
        Line2D([0], [0], color='blue', linestyle='--', linewidth=2, label='23-24 Vaccinations')
    )
    
    # Add data point elements
    legend_elements.extend([
        Line2D([0], [0], marker='o', color='w', markeredgecolor='black', 
               markersize=8, alpha=0.7, label='24-25 Data'),
        Line2D([0], [0], marker='o', color='w', markeredgecolor='red', 
               markersize=8, alpha=0.7, label='23-24 Appended Data')
    ])
    plt.legend(handles=legend_elements, loc='lower right', bbox_to_anchor=(0.98, 0.02))

    # Save the plot
    plt.savefig(os.path.join(scenario_dir, f'{state_nameid}-{age}-fit.pdf'), 
                bbox_inches='tight', dpi=300)
    plt.close()

    
def process_vax_data(df: pd.DataFrame):
    # Filter out rows where state is NA (equivalent to filter(!is.na(state)))
    df_filtered = df.dropna(subset=['state']).copy()
    # Filter out rows where prop_state is NA
    df_filtered = df_filtered.dropna(subset=['prop_state'])
    #drop adjusted column if exists
    if 'adjusted' in df_filtered.columns:
        df_filtered = df_filtered.drop(columns=['adjusted'])
    #rename columns
    df_filtered.columns = ['state', 'age', 'date', 'frac_vax']
    
    # Convert state abbreviations to name IDs and create categorical with specified levels
    df_filtered['state'] = df_filtered['state'].apply(state_abbrev_to_nameid)
    df_filtered['state'] = pd.Categorical(df_filtered['state'], categories=state_nameids, ordered=True)
     # Sort by state, age, and date (equivalent to arrange(state, age, date))
    df_processed = df_filtered.sort_values(['state', 'age', 'date']).reset_index(drop=True)
    return df_processed

def format_vax_2324(df: pd.DataFrame):
     # Rename specific age group (equivalent to covid_vax_2324[covid_vax_2324$Age == "6 months-17 years",]$Age <- "0-17")
    df.loc[df['Age'] == "6 months-17 years", 'Age'] = "0-17"
    # Remove " years" from Age column (equivalent to gsub(" years", "", covid_vax_2324$Age))
    df['Age'] = df['Age'].str.replace(" years", "", regex=False)
    # Convert Geography to state name IDs
    df['state'] = df['Geography'].apply(state_name_to_nameid)
    # Filter for "Overall" risk group and rename columns
    df = (df
        .query('Risk_group == "Overall"')
        .rename(columns={'Age': 'age', 'Date': 'date'}))
    # Subtract 1 day from date (equivalent to covid_vax_2324$date <- covid_vax_2324$date - 1)
    df['date'] = pd.to_datetime(df['date']) - pd.Timedelta(days=1)
    # Replace " – " with "-" in age column (equivalent to gsub(" – ", "-", covid_vax_2324$age))
    df['age'] = df['age'].str.replace(" – ", "-", regex=False)
    return df


def write_json(state_fit_results, scenario_dir, state_nameid):
    """
    Write the state fit results to a JSON file.
    
    Parameters:
    state_fit_results : dict
        Dictionary containing the fit results for the state.
    scenario_dir : str
        Directory where the JSON file will be saved.
    state_nameid : str
        Name ID of the state.
    """
    # Ensure the directory exists
    os.makedirs(scenario_dir, exist_ok=True)
    
    # Write the results to a JSON file
    with open(os.path.join(scenario_dir, f'{state_nameid}_hillvax_config.json'), 'w') as f:
        json.dump(state_fit_results, f, indent=4, ensure_ascii=False)



def fit_scenario_state_age(
    scenario_dir,
    state_nameid, 
    age,
    vax_df,
    start_date,
    final_ratio_date,
    final_frac_vax_state_2425
):
    """
    Fit vaccination curve for a specific state and age group.
    
    Parameters:
    -----------
    scenario_dir : str
        Directory where outputs will be saved
    state_nameid : str
        State name/identifier
    age : str
        Age group identifier
    vax_df : pd.DataFrame
        Vaccination data for this state and age group
    start_date : datetime or pd.Timestamp
        Start date for the vaccination campaign
    final_ratio_date : datetime or pd.Timestamp
        Date for the final anchor point
    final_frac_vax_state_2425 : float
        Final vaccination fraction for this state in 2024-25
    
    Returns:
    --------
    dict
        Dictionary containing fit results and parameters
    """
    
    # Ensure single state and age group
    assert len(vax_df['state'].unique()) == 1, "Data should contain only one state"
    assert all(vax_df['state'] == state_nameid), "All data should be for the specified state"
    assert len(vax_df['age'].unique()) == 1, "Data should contain only one age group"
    assert all(vax_df['age'] == age), "All data should be for the specified age group"
    
    # Convert dates to datetime if they're strings
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(final_ratio_date, str):
        final_ratio_date = pd.to_datetime(final_ratio_date)
    
    # Compute time where anchor point in curve is specified
    t_end = (final_ratio_date - start_date).days
    
    print(f'    fitting {state_nameid}, {age} (ratio 23-24 {final_frac_vax_state_2425:.6f}, t_end {t_end}, final frac {final_frac_vax_state_2425:.6f})')
    
    # Extract timeseries for fitting from data frame
    vax_df = vax_df.copy()
    vax_df['date'] = pd.to_datetime(vax_df['date'])
    t_obs = (vax_df['date'] - start_date).dt.days.values
    
    frac_vax_obs = vax_df['frac_vax'].values
    frac_prev_obs = vax_df['Cum_Coverage_Percent'].values
    
    # Compute final frac_vax
    final_frac_vax = final_frac_vax_state_2425
    
    # Create timeseries plot
    plot_timeseries(t_obs, frac_vax_obs, t_end, final_frac_vax, scenario_dir, state_nameid, age)
    
    # Fit the model
    fit_result = fit_vax_scaledhill_from_endpoint(
        t_obs,          # observation times
        frac_vax_obs,   # observed values
        t_end,          # time at which endpoint y-value is specified
        final_frac_vax  # endpoint y-value
    )
    
    # Create fit plot
    plot_fit(t_obs, frac_vax_obs, t_end, final_frac_vax, fit_result, y_prev=frac_prev_obs,
             scenario_dir=scenario_dir, state_nameid=state_nameid, age=age)
    
    
    # Check convergence
    if not fit_result['success']:
        print(f"Warning: Optimization did not converge for {state_nameid}, {age}")
        print(f"Fit result: {fit_result}")
    
    # Extract parameters
    shape = fit_result['par']['n']  # 'n' parameter
    t_halfsat = fit_result['par']['t_h']  # 't_h' parameter

    # Calculate scale parameter
    scale = (1 + (t_halfsat / t_end)**shape) * final_frac_vax
    
    # Return results dictionary
    return {
        'state': state_nameid,
        'ageclass': age,
        'shape': shape,
        'scale': scale,  # To identify nonsensical fits
        'start_date': start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date),
        't_halfsat': t_halfsat,
        't_anchor': t_end,
        'frac_vax_anchor': final_frac_vax
    }

def fit_scenario(
    vax_df,
    scenario_name, 
    final_national_frac_date,
    state_nameids,
    age_factor_levels,
    scenarios_dir='scenarios_new'
):
    """
    Fit vaccination curves for all states in a scenario.
    
    Parameters:
    -----------
    vax_df : pd.DataFrame
        Vaccination data for all states and age groups
    scenario_name : str
        Name of the scenario (e.g., 'low', 'med', 'high')
    final_national_frac_date : str or datetime
        Final date for the vaccination campaign
    state_nameids : list
        List of state name identifiers
    age_factor_levels : list
        List of age group identifiers
    scenarios_dir : str
        Directory where results will be saved (default: 'scenarios_new')
    
    Returns:
    --------
    None
        Writes results to JSON files
    """
    
    # Make directories
    if not os.path.exists(scenarios_dir):
        os.makedirs(scenarios_dir)
    
    scenario_dir = os.path.join(scenarios_dir, scenario_name)
    if not os.path.exists(scenario_dir):
        os.makedirs(scenario_dir)
    
    # Print data preview (equivalent to head(vax_df) in R)
    print("First few rows of vaccination data:")
    print(vax_df.head())
    
    # Fit for each state
    scenario_results = []
    
    for state_nameid in state_nameids:
        # Filter data for this state
        state_vax_df = vax_df[vax_df['state'] == state_nameid].copy()
        
        if state_vax_df.empty:
            print(f"Warning: No data found for state {state_nameid}")
            continue
        
        # Find the max vax for each age group from the final data point
        # Get the latest date for this state
        max_date = state_vax_df['date'].max()
        
        # Get final vaccination fractions for each age group
        final_data = state_vax_df[state_vax_df['date'] == max_date]
        final_state_frac_vax = []
        
        # Ensure we have data for all age groups
        for age in age_factor_levels:
            age_data = final_data[final_data['age'] == age]
            if not age_data.empty:
                final_state_frac_vax.append(age_data['frac_vax'].iloc[0])
            else:
                # If no data for this age group, use 0 as default
                print(f"Warning: No final data for {state_nameid}, age {age}")
                final_state_frac_vax.append(0.0)
        
        # Fit this state
        state_result = fit_scenario_state(
            scenario_name, 
            scenario_dir,
            state_nameid, 
            state_vax_df,
            final_national_frac_date, 
            final_state_frac_vax,
            age_factor_levels
        )
        
        scenario_results.append(state_result)
    
    # Write all states to one file
    with open(os.path.join(scenario_dir, '_allstates_hillvax_config.json'), 'w') as f:
        json.dump(scenario_results, f, indent=2, default=str)
    
    # Write problematic states to separate file
    problematic_results = []
    
    for state_fit in scenario_results:
        if 'fits' in state_fit:
            fits = state_fit['fits']
            is_problematic = False
            
            # Check each age group fit
            for i, age in enumerate(age_factor_levels):
                if i < len(fits):
                    fit_data = fits[i]
                    if (fit_data.get('scale', 0) > 1 or 
                        fit_data.get('t_halfsat', 0) > 1000):
                        is_problematic = True
                        break
            
            if is_problematic:
                problematic_results.append(state_fit)
    
    # Write problematic results
    if problematic_results:
        with open(os.path.join(scenario_dir, '_badstates_hillvax_config.json'), 'w') as f:
            json.dump(problematic_results, f, indent=2, default=str)
        
        print(f"Found {len(problematic_results)} problematic states with unusual fits")
    
    print(f"Scenario {scenario_name} done\n")


def fit_scenario_state(
    scenario_name,
    scenario_dir,
    state_nameid, 
    vax_df,
    final_national_frac_date, 
    final_state_frac_vax,
    age_factor_levels
):
    """
    Fit vaccination curves for a single state across all age groups.
    
    Parameters:
    -----------
    scenario_name : str
        Name of the scenario
    scenario_dir : str
        Directory for output files
    state_nameid : str
        State identifier
    vax_df : pd.DataFrame
        Vaccination data for this state
    final_national_frac_date : str or datetime
        Final date for vaccination campaign
    final_state_frac_vax : list
        Final vaccination fractions for each age group
    age_factor_levels : list
        List of age group identifiers
    
    Returns:
    --------
    dict
        Dictionary containing state results and fits
    """
    
    # Validate input
    assert len(vax_df['state'].unique()) == 1, "Data should contain only one state"
    
    print(f"  Fitting state {state_nameid}")
    
    # Fit each age group
    state_fit_results = []
    
    for i, age_i in enumerate(age_factor_levels):
        # Filter data for this age group
        age_vax_df = vax_df[vax_df['age'] == age_i].copy()
        
        if age_vax_df.empty:
            print(f"    Warning: No data for age group {age_i}")
            # Create a placeholder result
            placeholder_result = {
                'state': state_nameid,
                'ageclass': age_i,
                'shape': 0.0,
                'scale': 0.0,
                'start_date': final_national_frac_date,
                't_halfsat': 0.0,
                't_anchor': 0.0,
                'frac_vax_anchor': 0.0
            }
            state_fit_results.append(placeholder_result)
            continue
        
        # Get start date for this age group (you'll need to define vax_start_dates)
        # For now, using a default mapping - you should replace this with your actual vax_start_dates
        start_date_map = {
            '0-17': '2024-09-29',
            '18-49': '2024-09-01', 
            '50-64': '2024-09-01',
            '65+': '2024-09-01'
        }
        start_date = start_date_map.get(age_i, '2024-09-01')
        
        # Get max date from the data
        max_date = age_vax_df['date'].max()
        
        # Calculate final fraction, ensuring it doesn't exceed reasonable bounds
        final_frac = min(final_state_frac_vax[i] * 1.01, 0.98) if i < len(final_state_frac_vax) else 0.5
        
        # Fit this age group
        try:
            age_result = fit_scenario_state_age(
                scenario_dir,
                state_nameid, 
                age_i, 
                age_vax_df,
                start_date,
                max_date,
                final_frac
            )
            state_fit_results.append(age_result)
            
        except Exception as e:
            print(f"    Error fitting {state_nameid}, {age_i}: {e}")
            # Create a placeholder result for failed fits
            placeholder_result = {
                'state': state_nameid,
                'ageclass': age_i,
                'shape': 0.0,
                'scale': 0.0,
                'start_date': start_date,
                't_halfsat': 0.0,
                't_anchor': 0.0,
                'frac_vax_anchor': final_frac
            }
            state_fit_results.append(placeholder_result)
    
    # Write state results to individual file
    with open(os.path.join(scenario_dir, f'{state_nameid}_hillvax_config.json'), 'w') as f:
        json.dump(state_fit_results, f, indent=2, default=str)
    
    print(f"  ...state {state_nameid} done")
    
    return {
        'state': state_nameid,
        'fits': state_fit_results
    }

def add_vax_2324(vax_df: pd.DataFrame, covid_vax_2324: pd.DataFrame):
    """
    Left joins 2023-2024 vaccination data to the existing vaccination DataFrame.

    Args:
        vax_df (pd.DataFrame): vaccination DataFrame to which 2023-2024 data will be added.
        covid_vax_2324 (pd.DataFrame): vaccination DataFrame for 2023-2024 data.

    Returns:
        pd.DataFrame: Updated vaccination DataFrame with 2023-2024 data added.
    """
    vax_df['date'] = vax_df['date'].astype(str)
    covid_vax_2324['date'] = covid_vax_2324['date'].astype(str)
    combined_df = vax_df.merge(covid_vax_2324,
        how='left',
        on=['state', 'age', 'date'])
    return combined_df