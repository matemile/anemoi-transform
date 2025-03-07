# (C) Copyright 2024 Anemoi contributors.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

from typing import Iterator

import earthkit.data as ekd
import numpy as np

from anemoi.transform.filters import filter_registry
from anemoi.transform.filters.matching import MatchingFieldsFilter
from anemoi.transform.filters.matching import matching

def pp_radar(lwe: np.ndarray) -> np.ndarray:
    print("✅✅", lwe)

    lwe[lwe == np.nan] = 0

    return lwe

@filter_registry.register("nordic_pp")
class NordicPP(MatchingFieldsFilter):
    """A filter to remove all nans in Nordic RADAR data.

    Parameters
    ----------
    lwe : str, optional
        The name of the precip field.
    output : str, optional
        The name of the output field.
    """

    @matching(
        match="param",
        forward=("lwe"),
    )
    def __init__(
        self,
        *,
        lwe: str = "lwe",
        output: str = "lwe_pp",
    ) -> None:
        """Initialize the NordicPP filter.

        Parameters
        ----------
        tp : str, optional
            The name of the tp field, by default "tp".
        mask : str, optional
            The name of the mask field, by default "mask".
        output : str, optional
            The name of the output field, by default "tp_cleaned".
        max_tp : int, optional
            The maximum value for tp, by default MAX_TP.
        """
        self.lwe = lwe
        self.lwe_pp = output

    def forward_transform(
        self,
        lwe: ekd.Field,
    ) -> Iterator[ekd.Field]:
        """Pre-process Nordic RADAR data.

        Parameters
        ----------
        lwe : ekd.Field
            The precip data.

        Returns
        -------
        Iterator[ekd.Field]
            Transformed fields.
        """
        # 1st - apply masking
        #tp_masked = mask_opera(tp=tp.to_numpy(), quality=quality.to_numpy(), mask=mask.to_numpy())

        # 2nd - apply clipping
        #tp_cleaned, quality = clip_opera(tp=tp_masked, quality=quality.to_numpy(), max_tp=self.max_tp)

        # apply nan removal
        lwe_pp = pp_radar(lwe=lwe.to_numpy())

        yield self.new_field_from_numpy(lwe_pp, template=lwe, param=self.lwe_pp)
