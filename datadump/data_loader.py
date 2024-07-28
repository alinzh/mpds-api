import json
import os.path

import pandas as pd
from mpds_client import MPDSDataRetrieval


class MPDS_Fields:
    schema = json.load(open("./schema.json"))
    properties = ["physical properties", "phase diagram"]
    crystal_structures_fields = schema["definitions"]["crystal_structure"][
        "properties"
    ].keys()
    physical_fields = [
        "sample.material.chemical_formula",
        "sample.material.chemical_elements",
        "sample.material.condition",
        "sample.material.phase",
        "sample.material.phase_id",
        "sample.material.entry",
        "sample.material.object_repr",
        "sample.measurement.property",
        "sample.measurement.condition",
        "sample.measurement.raw_data",
    ]
    diagram_fields = schema["definitions"]["phase_diagram"]["properties"].keys()


class DataLoaderMPDS:
    """
    Make requests to MPDS database, save data in csv format.
    """

    def __init__(self, dtype: int = 7, api_key: str = None) -> None:
        """
        dtype : int
            Type of receiving data. Available: PEER_REVIEWED = 1, MACHINE_LEARNING = 2,
            AB_INITIO = 4, ALL = 7
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
        for date in range(1965, 2018):
            if not os.path.isfile(f"./data/atomic_structure_{date}.csv"):
                try:
                    dfrm = pd.DataFrame(
                        self.client.get_data(
                            {"props": "atomic structure", "years": str(date)},
                            fields={"S": MPDS_Fields.crystal_structures_fields},
                        ),
                        columns=MPDS_Fields.crystal_structures_fields,
                    )
                    dfrm.to_csv(f"./data/atomic_structure_{date}.csv")
                    print(dfrm)
                except Exception as error:
                    print("An exception occurred:", error)

    def request_phase_diagram(self):
        """
        Request atomic structure. Save in dir './data'
        """
        print("---Started receiving: phase diagram---")
        for date in range(1965, 2018):
            if not os.path.isfile(f"./data/phase_diagram_{date}.csv"):
                try:
                    dfrm = pd.DataFrame(
                        self.client.get_data(
                            {"props": "phase diagram", "years": str(date)},
                            fields={"C": MPDS_Fields.diagram_fields},
                        ),
                        columns=MPDS_Fields.diagram_fields,
                    )
                    dfrm.to_csv(f"./data/phase_diagram_{date}.csv")
                    print(dfrm)
                except Exception as error:
                    print("An exception occurred:", error)

    def request_phys_properties(self):
        """
        Request physical properties. Save in dir './data'
        """
        print("---Started receiving: physical properties---")
        for date in range(1965, 2018):
            if not os.path.isfile(f"./data/physical_property_{date}.csv"):
                try:
                    dfrm = pd.DataFrame(
                        self.client.get_data(
                            {"props": "physical properties", "years": str(date)},
                            fields={"P": MPDS_Fields.physical_fields},
                        )
                    )
                    dfrm.columns = MPDS_Fields.physical_fields
                    dfrm.to_csv(f"./data/physical_property_{date}.csv")
                    print(dfrm)
                except Exception as error:
                    print("An exception occurred:", error)

    def request_all_data(self):
        """
        Run getting all data
        """
        self.request_phys_properties()
        self.request_structure()
        self.request_phase_diagram()


if __name__ == "__main__":
    client = DataLoaderMPDS(api_key="KEY")
    client.request_all_data()
