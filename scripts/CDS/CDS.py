import os
import re

import pandas as pd


def load_CDS(language="NL"):
    """Loads the cognitive distortion schemata (CDS) dataframe.

        Returns
        -------
        CDS : pandas.DataFrame
            A dataframe that contains all CDS, along with their categories and
            possible variants (e.g. the CDS "I am a" has the variants "I'm a")
    """
    base_dir = os.path.dirname(__file__)
    fn = os.path.join(base_dir, "translations", f"list_of_CDS_{language}.tsv")
    
    if not os.path.isfile(fn):
        raise(NotImplementedError("No translation available."))

    df = pd.read_csv(fn, sep="\t", index_col="markers", dtype=str)
    df.loc[df["variants"].isna(), "variants"] = "[]"
    df["variants"] = df["variants"].apply(eval)
    return df


def find_CDS_in_text(cds, variants, text):
    """Searches for a specific CDS and its variants in a text

        Parameters
        ----------
        cds: string
            The CDS that needs to be matched in the text.
        variants: list of strings
            Possible spelling variants of the CDS (e.g., "Im" for "I am").
        text: string
            The text in which we want to find the CDS.

        Returns
        -------
        int
            Returns a 1 if the tweet text contains the specific CDS or 0 otherwise
    """
    if re.search(r"\b{}\b".format(cds.lower()), text):
        return 1

    if variants != []:
        for var in variants:
            if re.search(r"\b{}\b".format(var.lower()), text):
                return 1
    return 0


def find_CDS(tweet, CDS=None):
    """Locates the CDS in a tweet text through string matching.

        Parameters
        ----------
        tweet: pandas.Series
            tweet information (must contain a 'text'-column, which is assumed to be lowercased)
        CDS: pandas.DataFrame
            cognitive distortion schemata (CDS) dataframe

        Returns
        -------
        output : pandas.Series
            pandas Series of integers that indicate whether the tweet text
            contains each specific CDS (1 if it does, 0 otherwise).
    """
    output = pd.Series(index=CDS.index)
    for cds in CDS.index:
        output[cds] = find_CDS_in_text(cds, CDS.loc[cds, "variants"], tweet.text)
    return output


def process_categories(row, CDS=None):
    """Locates the CDS categories in a tweet text.

        Parameters
        ----------
        row: pandas.Series
            per CDS matching of each tweet text
        CDS: pandas.DataFrame
            cognitive distortion schemata (CDS) dataframe

        Returns
        -------
        output : pandas.Series
            pandas Series of booleans that indicate whether the tweet text
            contains each at least one CDS within the given category of CDS
    """
    output = pd.Series(index=CDS.categories.unique(), data=0)
    categories = CDS[row == 1].categories.unique()
    output[categories] = 1
    return output


def process_dataset(tweets, output="per_tweet", language="NL"):
    """Processes a collection of tweets to locate CDS in the tweet texts.

        Parameters
        ----------
        tweets : pandas.DataFrame of tweets
            needs to contain a column named 'text' with the tweet lowercased texts.
        output: string (default "per_tweet")
            defines the output of the function. There are four options:
            1. per_tweet: returns a dataframe with a boolean per document whether it contains any CDS
            2. per_category: returns a dataframe with a boolean per document whether it contains a CDS of a category
            3. per_phrase: returns a dataframe with a boolean per document whether it contains a specific CDS
            4. all_variants: returns all previously mentioned outputs
        language: string (default "EN")
            specifies the language in which the annotation of the CDS is performed. Currently,
            the options English (EN), Dutch (NL), German (DE) and Spanish (ES) are supported.
    """
    if "text" not in tweets.columns:
        raise(ValueError("Tweets dataframe should contain a column named 'text'!"))

    if output not in ["per_phrase", "per_category", "per_tweet", "all_variants"]:
        e_str = 'This output type is not implemented. Choose one of the following:'
        e_str2 = '["per_phrase", "per_category", "per_tweet", "all_variants"]'
        raise(NotImplementedError("{} {}".format(e_str, e_str2)))

    CDS = load_CDS(language=language)

    CDS_phrases = tweets.apply(find_CDS, axis=1, CDS=CDS)
    if output == "per_phrase":
        return CDS_phrases
    else:
        CDS_categories = CDS_phrases.apply(process_categories, axis=1, CDS=CDS)
        if output == "per_category":
            return CDS_categories
        else:
            CDS_per_tweet = CDS_categories.sum(axis=1) > 0
            CDS_per_tweet.name = "CDS"
            if output == "per_tweet":
                return CDS_per_tweet.to_frame()
            else:
                return CDS_phrases, CDS_categories, CDS_per_tweet.to_frame()
            

            # python scripts/CDS/test_cds.py
