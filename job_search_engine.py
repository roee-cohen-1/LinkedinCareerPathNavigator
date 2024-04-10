import pandas as pd
from retriv import SparseRetriever


def _get_retriever(recreate=False):
    try:
        if recreate:
            raise Exception()
        return SparseRetriever.load(index_name="job-index")
    except:
        sr = SparseRetriever(
            index_name="job-index",
            model="bm25",
            min_df=1,
            tokenizer="whitespace",
            stemmer="english",
            stopwords="english",
            do_lowercasing=True,
            do_ampersand_normalization=True,
            do_special_chars_normalization=True,
            do_acronyms_normalization=True,
            do_punctuation_removal=True,
        )
        sr = sr.index_file(
            path="job_collection.jsonl",
            show_progress=True
        )
    return sr


def retrieve(query):
    sr = _get_retriever()
    return sr.search(query=query, return_docs=True, cutoff=3)


def run(query):
    print('Results for', query)
    for job in retrieve(query):
        print('\t', job['id'], job['title'])


if __name__ == '__main__':
    print('margedady')
    run('Product Manager, Data Analytics, Digital Product Management, Agile Development')
    run('Vice President of Product Management, Product Management, Digital Marketing, Business Strategy')
    run('E-commerce Product & Project Manager, E-commerce Product Management, Project Management, Website Improvements, Cross-Channel Customer Experience, Agile Development')
    print()
    print('venmathi-mark-8105b2104')
    run('Big Data Architect, Hadoop, Apache Spark, Data Engineering')
    run('Chief Data Scientist, Cloud Computing, Machine Learning, Big Data')
    print()
    print('angel-fong-161baa112')
    run('Senior Environmental Scientist, Environmental Consulting, Environmental Impact Assessment, Sustainability')
    run('Postdoctoral Research Associate, Data Analysis, Climate Science, Crop Modeling')
    run('Environmental Scientist, Environmental remediation, Groundwater management, Water quality analysis')
    print()
    print('miguel-reyes-41162815b')
    run('Clinical Project Manager, Clinical Research Management, Regulatory Compliance, Project Planning and Execution, Clinical Data Analysis, Site Management')
    run('Senior Clinical Research Associate III, Monitoring, Clinical Data Management, Quality Assurance')
    run('Senior Clinical Trial Manager, Clinical Trial Management, Data Management, Project Management')

