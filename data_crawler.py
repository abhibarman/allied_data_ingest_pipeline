from bs4 import BeautifulSoup
import requests
from langchain.document_loaders import UnstructuredURLLoader
import pandas as pd
from clearml import PipelineController

def get_pages(url = "https://awac.com/"):
    import requests
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = [a.get('href') for a in soup.find_all('a')]

    links_formatted = []
    for link in links:
        if link:                
            if link.startswith('/') or url in link:
                links_formatted.append(link)
    links_formatted = list(dict.fromkeys(links_formatted))

    for idx,link in enumerate(links_formatted):
        if link.startswith('/'):
            links_formatted[idx] = url[:-1]+link

    links_formatted = links_formatted[:10]

    return links_formatted

def load_from_urls(url = "https://awac.com/"):

    urls = get_pages(url)

    data_list = []
    loader = UnstructuredURLLoader(urls=urls)
    data = loader.load()

    for doc in data:
        data_dict = {

            "Source": doc.metadata["source"],
            "page_content": doc.page_content
        }
        data_list.append(data_dict)
    dataframe = pd.DataFrame(data_list)

    return dataframe

#df = load_from_urls()
#print(df.head())


if __name__ == '__main__':

    # create the pipeline controller
    pipe = PipelineController(
        project='AWAC',
        name='AWAC_Data_Ingest',
        version='1.1',
        add_pipeline_tags=False,
    )

    # set the default execution queue to be used (per step we can override the execution)
    pipe.set_default_execution_queue('default')

    # add pipeline components
    pipe.add_parameter(
        name='url',
        description='url to crawl from',
        default='https://awac.com/'
    )
    pipe.add_function_step(
        name='get_pages',
        function=get_pages,
        function_kwargs=dict(url='${pipeline.url}'),
        function_return=['links_formatted'],
        cache_executed_step=True,
    )
    pipe.add_function_step(
        name='load_from_urls',
        #parents=['get_pages'],  # the pipeline will automatically detect the dependencies based on the kwargs inputs
        function=load_from_urls,
        function_kwargs=dict(data_frame='${get_pages.links_formatted}'),
        function_return=['dataframe'],
        cache_executed_step=True,
    )


    # For debugging purposes run on the pipeline on current machine
    # Use run_pipeline_steps_locally=True to further execute the pipeline component Tasks as subprocesses.
    # pipe.start_locally(run_pipeline_steps_locally=False)

    # Start the pipeline on the services queue (remote machine, default on the clearml-server)
    pipe.start()

    print('pipeline completed')
