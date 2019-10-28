from xml.etree import ElementTree as ET
import io
import os.path
import sys

import gpxpy
import builtins


def find_all_tags(fp, tags, progress_callback=None):
    parser = ET.XMLPullParser(("start", "end"))
    root = None
    while True:
        chunk = fp.read(1024 * 1024)
        if not chunk:
            break
        parser.feed(chunk)
        for event, el in parser.read_events():
            if event == "start" and root is None:
                root = el
            if event == "end" and el.tag in tags:
                yield el.tag, el
            root.clear()
        if progress_callback is not None:
            progress_callback(len(chunk))


def convert_xml_to_sqlite(fp, db, progress_callback=None, zipfile=None):
    activity_summaries = []
    records = []
    workout_id = 1
    for tag, el in find_all_tags(
        fp, {"Record", "Workout", "ActivitySummary"}, progress_callback
    ):
        if tag == "ActivitySummary":
            activity_summaries.append(dict(el.attrib))
            if len(activity_summaries) >= 100:
                db["activity_summary"].insert_all(activity_summaries)
                activity_summaries = []
        elif tag == "Workout":
            el.set("seq", workout_id)
            workout_to_db(el, db, zipfile)
            workout_id += 1
        elif tag == "Record":
            record = dict(el.attrib)
            for child in el.findall("MetadataEntry"):
                record["metadata_" + child.attrib["key"]] = child.attrib["value"]
            records.append(record)
            if len(records) >= 200:
                write_records(records, db)
                records = []
        el.clear()
    if records:
        write_records(records, db)
    if activity_summaries:
        db["activity_summary"].insert_all(activity_summaries)
    if progress_callback is not None:
        progress_callback(sys.maxsize)


def workout_to_db(workout, db, zf):
    record = dict(workout.attrib)
    # add metadata entry items as extra keys
    for el in workout.findall("MetadataEntry"):
        record["metadata_" + el.attrib["key"]] = el.attrib["value"]
    # Dump any WorkoutEvent in a nested list for the moment
    record["workout_events"] = [el.attrib for el in workout.findall("WorkoutEvent")]
    pk = db["workouts"].insert(record, alter=True, hash_id="id").last_pk
    points = [
        dict(el.attrib, workout_id=pk)
        for el in workout.findall("WorkoutRoute/Location")
    ]
    if len(points) == 0:
        # Location not embedded, sidecar gpx files used instead
        gpx_files = [os.path.join("apple_health_export", *(item.get("path").split("/")))
                     for item in workout.findall("WorkoutRoute/FileReference")]
        # support zip or flat files
        for path in gpx_files:
            with open_file_or_zip(zf, path) as xml_file:
                gpx = parse_gpx(xml_file)
                for point in gpx.walk(only_points=True):
                    points.append(dict(vars(point), workout_id=pk))
    if len(points):
        db["workout_points"].insert_all(
            points, foreign_keys=[("workout_id", "workouts")], batch_size=50
        )


def open_file_or_zip(zf, file):
    if zf is not None:
        return zf.open(file)
    else:
        return builtins.open(file, 'rb')


def parse_gpx(xml_file):
    doc = io.TextIOWrapper(xml_file, encoding='UTF-8', newline=None)
    doc.readline()  # skip xml header
    return gpxpy.parse("".join(doc.readlines()))


def write_records(records, db):
    # We write records into tables based on their types
    records_by_type = {}
    for record in records:
        table = "r{}".format(
            record.pop("type")
            .replace("HKQuantityTypeIdentifier", "")
            .replace("HKCategoryTypeIdentifier", "")
        )
        records_by_type.setdefault(table, []).append(record)
    # Bulk inserts for each one
    for table, records_for_table in records_by_type.items():
        db[table].insert_all(
            records_for_table,
            alter=True,
            column_order=["startDate", "endDate", "value", "unit"],
            batch_size=50,
        )
