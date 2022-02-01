#!/usr/bin/env python3
#
############################################################################
#
# MODULE:       r.import.worker
# AUTHOR(S):    Anika Weinmann
# PURPOSE:      This is a worker addon to run r.import in different mapsets
# COPYRIGHT:    (C) 2020-2022 by mundialis GmbH & Co. KG and the GRASS
#               Development Team
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
############################################################################

# %module
# % description: Runs r.import as a worker in different mapsets.
# % keyword: raster
# % keyword: import
# % keyword: projection
# % keyword: parallel
# %end

# %option
# % key: newmapset
# % type: string
# % required: yes
# % multiple: no
# % key_desc: name
# % description: Name of new mapset to run i.sentinel.mask
# % guisection: Required
# %end

# %flag
# % key: e
# % description: Estimate resolution only
# % guisection: Optional
# %end

# %flag
# % key: n
# % description: Do not perform region cropping optimization
# % guisection: Optional
# %end

# %flag
# % key: l
# % description: Force Lat/Lon maps to fit into geographic coordinates (90N,S; 180E,W)
# %end

# %flag
# % key: o
# % label: Override projection check (use current location's projection)
# % description: Assume that the dataset has the same projection as the current location
# %end

# %option
# % key: input
# % type: string
# % required: yes
# % multiple: no
# % key_desc: name
# % description: Name of GDAL dataset to be imported
# % gisprompt: old,bin,file
# % guisection: Input
# %end

# %option
# % key: band
# % type: integer
# % required: no
# % multiple: yes
# % description: Input band(s) to select (default is all bands)
# % guisection: Input
# %end

# %option G_OPT_MEMORYMB
# %end
# %option
# % key: output
# % type: string
# % required: no
# % multiple: no
# % key_desc: name
# % description: Name for output raster map
# % gisprompt: new,cell,raster
# % guisection: Output
# %end

# %option
# % key: resample
# % type: string
# % required: no
# % multiple: no
# % options: nearest,bilinear,bicubic,lanczos,bilinear_f,bicubic_f,lanczos_f
# % description: Resampling method to use for reprojection
# % descriptions: nearest;nearest neighbor;bilinear;bilinear interpolation;bicubic;bicubic interpolation;lanczos;lanczos filter;bilinear_f;bilinear interpolation with fallback;bicubic_f;bicubic interpolation with fallback;lanczos_f;lanczos filter with fallback
# % answer: nearest
# % guisection: Output
# %end

# %option
# % key: extent
# % type: string
# % required: no
# % multiple: no
# % options: input,region
# % description: Output raster map extent
# % descriptions: region;extent of current region;input;extent of input map
# % answer: input
# % guisection: Output
# %end

# %option
# % key: resolution
# % type: string
# % required: no
# % multiple: no
# % options: estimated,value,region
# % description: Resolution of output raster map (default: estimated)
# % descriptions: estimated;estimated resolution;value;user-specified resolution;region;current region resolution
# % answer: estimated
# % guisection: Output
# %end

# %option
# % key: resolution_value
# % type: double
# % required: no
# % multiple: no
# % description: Resolution of output raster map (use with option resolution=value)
# % guisection: Output
# %end

# %option
# % key: title
# % type: string
# % required: no
# % multiple: no
# % key_desc: phrase
# % description: Title for resultant raster map
# % guisection: Metadata
# %end

import os
import shutil
import subprocess
import sys
from time import sleep
import grass.script as grass


def main():

    # set some common environmental variables, like:
    os.environ.update(
        dict(
            GRASS_COMPRESS_NULLS="1",
            GRASS_COMPRESSOR="LZ4",
            GRASS_MESSAGE_FORMAT="plain",
        )
    )

    # actual mapset, location, ...
    env = grass.gisenv()
    gisdbase = env["GISDBASE"]
    location = env["LOCATION_NAME"]
    # old_mapset = env['MAPSET']

    new_mapset = options["newmapset"]
    grass.message(_("New mapset: <%s>" % new_mapset))
    grass.utils.try_rmdir(os.path.join(gisdbase, location, new_mapset))

    # create a private GISRC file for each job
    gisrc = os.environ["GISRC"]
    newgisrc = "%s_%s" % (gisrc, str(os.getpid()))
    grass.try_remove(newgisrc)
    shutil.copyfile(gisrc, newgisrc)
    os.environ["GISRC"] = newgisrc

    # save region if extent=region
    if options["extent"] == "region":
        reg = grass.region()

    # change mapset
    grass.message(_("GISRC: <%s>" % os.environ["GISRC"]))
    grass.run_command("g.mapset", flags="c", mapset=new_mapset)

    # set region
    if options["extent"] == "region":
        del reg["cells"]
        del reg["rows"]
        del reg["cols"]
        del reg["zone"]
        del reg["projection"]
        grass.run_command("g.region", **reg, flags="p")

    # import data
    grass.message(_("Running r.import ..."))
    kwargs = dict()
    for opt, val in options.items():
        if opt != "newmapset" and val:
            kwargs[opt] = val
    flagstr = ""
    for flag, val in flags.items():
        if val:
            flagstr += flag

    kwargsstr = ""
    for key, val in kwargs.items():
        kwargsstr += " %s='%s'" % (key, val)
    max_tries = 10
    next_try = True
    tries = 0
    noOverlap = False
    while next_try:
        tries += 1
        cmd = grass.Popen(
            "r.import --q %s" % kwargsstr,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        resp = cmd.communicate()
        resp_text = ""
        for resp_line in resp:
            resp_text += resp_line.decode("utf-8")
        if (
            "\n0..3..6..9..12..15..18..21..24..27..30..33..36..39..42..45..48"
            "..51..54..57..60..63..66..69..72..75..78..81..84..87..90..93..96"
            "..99..100\n"
            in resp_text
        ):
            next_try = False
        elif (
            "Input raster does not overlap current computational region"
            in resp_text
        ):
            grass.warning(
                _(
                    "Input raster map <%s> does not overlap with current "
                    "computational region"
                )
                % options["input"]
            )
            next_try = False
            noOverlap = True
        elif (
            "The reprojected raster <%s> is empty" % options["output"]
            in resp_text
        ):
            grass.warning(
                _(
                    "Only no-data values found in current region for input "
                    f"raster <{options['input']}>"
                )
            )
            next_try = False
        elif "cpl_vsil_gzip.cpp" in resp_text and tries < max_tries:
            sleep(10)
            next_try = True
            msg = "Retrying %d/%d (%s) ..." % (
                tries,
                max_tries,
                options["input"],
            )
            grass.warning(msg)
        elif "503" in resp_text:
            msg = resp_text + " (%s)" % options["input"]
            if tries < max_tries:
                sleep(10)
                next_try = True
                msg += " Retrying %d/%d ..." % (tries, max_tries)
            else:
                next_try = False
            grass.warning(msg)
        elif resp_text != "":
            msg = f"{resp_text} ({options['input']})"
            if tries < max_tries:
                sleep(10)
                next_try = True
                msg += f" Retrying {tries}/{max_tries} ..."
            else:
                next_try = False
            grass.warning(msg)
        else:
            next_try = False

    if (
        not grass.find_file(
            name=options["output"],
            element="raster",
            mapset=options["newmapset"],
        )["file"]
        and not grass.find_file(
            name=options["output"],
            element="group",
            mapset=options["newmapset"],
        )["file"]
        and noOverlap is not True
    ):
        grass.fatal(_("ERROR %s" % options["output"]))

    grass.utils.try_remove(newgisrc)
    os.environ["GISRC"] = gisrc
    return 0


if __name__ == "__main__":
    options, flags = grass.parser()
    sys.exit(main())
