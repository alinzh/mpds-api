import os.path

import pandas as pd
from mpds_client import MPDSDataRetrieval


class DataLoaderMPDS:
    """
    Make requests to MPDS database, save data in json format
    """

    def __init__(self, dtype: int = 7, api_key: str = None) -> None:
        """
        dtype : int
            Type of receiving data. Available: PEER_REVIEWED = 1,
            MACHINE_LEARNING = 2, AB_INITIO = 4, ALL = 7
        api_key : str
            Key from MPDS-account
        """
        self.api_key = api_key
        self.client = MPDSDataRetrieval(dtype=dtype, api_key=api_key)
        self.client.chillouttime = 1

    def request_structure(self):
        """
        Request atomic structure. Save in dir './data'
        """
        print("---Started receiving: atomic structure---")

        store = None
        for date in range(1965, 2018):
            if not os.path.isfile(f"./data/atomic_structure.json"):
                try:
                    dfrm = pd.DataFrame(
                        self.client.get_data(
                            {"props": "atomic structure", "years": str(date)},
                            fields={},
                        )
                    )
                    if not (isinstance(store, pd.DataFrame)):
                        store = dfrm
                    else:
                        store = pd.concat([store, dfrm], ignore_index=True)
                except Exception as error:
                    print("An exception occurred:", error)
        store.to_json(f"./data/atomic_structure.json")
        print("Successfully saved to './data/atomic_structure.json'")

    def request_phase_diagram(self):
        """
        Request atomic structure. Save in dir './data'
        """
        print("---Started receiving: phase diagram---")

        store = None
        for date in range(1965, 2018):
            if not os.path.isfile(f"./data/phase_diagram.json"):
                try:
                    dfrm = pd.DataFrame(
                        self.client.get_data(
                            {"props": "phase diagram", "years": str(date)}, fields={}
                        )
                    )
                    if not (isinstance(store, pd.DataFrame)):
                        store = dfrm
                    else:
                        store = pd.concat([store, dfrm], ignore_index=True)
                except Exception as error:
                    print("An exception occurred:", error)
        store.to_json(f"./data/phase_diagram.json")
        print("Successfully saved to './data/phase_diagram.json'")

    def request_phys_properties(self):
        """
        Request physical properties. Save in dir './data'
        """
        print("---Started receiving: physical properties---")

        store = None
        for date in range(1965, 2018):
            if not os.path.isfile("./data/physical_property.json"):
                try:
                    dfrm = pd.DataFrame(
                        self.client.get_data(
                            {"props": "physical properties", "years": str(date)},
                            fields={},
                        )
                    )
                    if not (isinstance(store, pd.DataFrame)):
                        store = dfrm
                    else:
                        store = pd.concat([store, dfrm], ignore_index=True)
                except Exception as error:
                    print("An exception occurred:", error)
        store.to_json(f"./data/physical_property.json")
        print("Successfully saved to './data/physical_property.json'")

    def request_all_data(self):
        """
        Run getting all data
        """
        self.request_phys_properties()
        self.request_structure()
        self.request_phase_diagram()


if __name__ == "__main__":
    client = DataLoaderMPDS(api_key="MPDS_KEY")
    client.request_all_data()
