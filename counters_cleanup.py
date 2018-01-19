import pandas as pd
import glob
from tqdm import tqdm
import logging

"""

counters_cleanup.py is used to identify which counters that are present in a specific source dataset are also present in
another target dataset. Only the needed columns (CONTER NUMBER/NAME) are read into a dataframe and then are all appended
into a list. The list is then concatenated so as to include all the counters found in all the .csv files and with
no duplicates. The final dataframe is then compared to the .csv target dataset file.


"""


logging.basicConfig(level=logging.INFO)


def print_full(fl):
    # *****************  Helper Function  **********************
    # *****************  Outputs full data-set  ****************
    print("########################### Printing full set of counters #########################")
    print("")

    with pd.option_context("display.max_rows", len(fl), "display.max_columns", None):
        print fl

    print("########################### End print #############################################")
    print("")

    return


def preprocess_source_counters():
    """
    This function loads into the all_data structure only specific columns from the corresponding .csv files
    list. It then proceeds to remove duplicates from specified column(s).

    """
    df_list = []
    values = ["COUNTER NUMBER"]

    logging.info("Retrieving source files...")
    path = r'/root/Desktop/Counters/source_files'
    all_data = glob.glob(path + "/*.csv")
    logging.info("Done!")

    logging.info("Processing source files...")
    for file_ in tqdm(all_data):
        df = pd.read_csv(file_, header=0, usecols=values)
        df_list.append(df)

    frames = pd.concat(df_list, ignore_index=True)
    frames.drop_duplicates(subset=values, keep="first",
                           inplace=True)

    return frames


def convert_target_counters(x):
    """

    This function converts the FxxxGxxx xml format to the final format. E.g: F29G034 -> 29034,
    F08G069 -> 8069.

    """

    if x[1] == str(0):  # If the counter in question is of the F0xx format -> remove 0 and keep the rest of the numbers
        cpy = x.replace("F", "").replace("G", "").replace("B", "").replace("0", "", 1)
    else:  # If the counter is of the Fxxx format keep all the numbers (F29G034 -> 29034)
        cpy = x.replace("F", "").replace("G", "").replace("B", "")

    return cpy


def process_target_counters():
    """

    This function extracts the target counters and converts them to the appropriate format using the
    convert_target_counters function. It then converts the "object" dtype records of the output dataframe (df2) to
    "numeric" dtype in order for the df2 to have the same type of records as the stabi_dataframe.

    """

    df1 = pd.read_csv("target_counters.csv", header=None)
    df2 = df1.ix[:, 1].apply(convert_target_counters).to_frame()

    df2.rename(columns={1: "COUNTER NUMBER"}, inplace=True)
    df2["COUNTER NUMBER"] = df2["COUNTER NUMBER"].apply(pd.to_numeric)
    # print df2["COUNTER NUMBER"].dtype
    logging.info("Done!")

    return df2


def output_selected_counters(source_dframe, target_dframe):
    """

    This function outputs counters to a .csv file. Just uncomment the desired option based on its respective comments.
    For example, uncommenting line 108 prints the source counters that aren't included in the target counters.

    """

    # Find which targets counters are present in the source counters too
    # fin_dframe = target_dframe.loc[target_dframe["COUNTER NUMBER"].isin(source_dframe["COUNTER NUMBER"])]

    # Find which source counters are not part of the target counters
    # fin_dframe = source_dframe.loc[~source_dframe["COUNTER NUMBER"].isin(target_dframe["COUNTER NUMBER"])]

    # Find which source counters are part of the target counters
    # fin_dframe = source_dframe.loc[source_dframe["COUNTER NUMBER"].isin(target_dframe["COUNTER NUMBER"])]

    # Find which target counters are not present in source counters
    fin_dframe = target_dframe.loc[~target_dframe["COUNTER NUMBER"].isin(source_dframe["COUNTER NUMBER"])]

    logging.info("File (output_counters.csv) created in the current directory.")
    fin_dframe.to_csv("output_counters.csv", sep=" ", index=False)

    return


if __name__ == "__main__":

    source_df = preprocess_source_counters()

    target_df = process_target_counters()

    output_selected_counters(source_df, target_df)

