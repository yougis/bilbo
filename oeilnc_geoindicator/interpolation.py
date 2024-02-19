from tobler.area_weighted import area_interpolate, area_join
import logging
#from warnings import deprecated
from oeilnc_config import settings

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S', level=logging.INFO)

logging.info("Utils - Interpolation Imported")

#@deprecated('Methode non fonctionnelle')
def indicateur_from_interpolation(ddf,iterables):
    # A refaire 
    # Deprecated
    data_to_interpolate, intensive_variables, extensive_variables, allocate_total = iterables
    interpolation = area_interpolate(source_df=data_to_interpolate, target_df=ddf, intensive_variables=intensive_variables, extensive_variables=extensive_variables,allocate_total=allocate_total)

    return interpolation


def indicateur_from_pre_interpolation(source_df, target_df, intensive_variables, extensive_variables, allocate_total, keepList, indexRef):
    """
    Perform pre-interpolation on the source dataframe and join the result with the target dataframe.

    Args:
        source_df (pandas.DataFrame): The source dataframe containing the data to be interpolated.
        target_df (pandas.DataFrame): The target dataframe to join the interpolated data with.
        intensive_variables (list): List of column names representing intensive variables.
        extensive_variables (list): List of column names representing extensive variables.
        allocate_total (bool): Flag indicating whether to allocate the total value.
        keepList (list): List of column names to keep from the target dataframe.
        indexRef (str): Name of the column to use as the index for joining.

    Returns:
        pandas.DataFrame: The pre-interpolated dataframe with the joined columns.

    """
    pre_interpolation = area_interpolate(source_df=source_df, target_df=target_df, intensive_variables=intensive_variables, extensive_variables=extensive_variables, allocate_total=allocate_total)
    columns = keepList + [indexRef]
    logging.info(f"Les colonnes de pré-interpolation sont : {pre_interpolation.columns}")
    pre_interpolation = area_join(target_df, pre_interpolation, columns)
    pre_interpolation.set_index(indexRef, inplace=True, drop=False)
    pre_interpolation.rename(columns={indexRef: f'{indexRef}_interpolation'}, inplace=True)
    print("finish join", pre_interpolation.columns)

    return pre_interpolation
