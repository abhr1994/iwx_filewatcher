from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.compute import Functions
from diagrams.onprem.inmemory import Redis
from diagrams.gcp.storage import Storage
from diagrams.custom import Custom

with Diagram("File Watcher", show=True):
    with Cluster(""):
        iwx = Custom("", "./my_resources/iwx.png")
        Storage("Files landing \n in bucket") >> Edge(color="brown", label="Process Files") >> Functions(
            "Cloud Function") >> Edge(color="brown",
                                      label="Check for Workflow Lock") >> Redis("userdb") >> Edge(
            label="Run Infoworks Workflow", color="brown", style="dashed") >> iwx
