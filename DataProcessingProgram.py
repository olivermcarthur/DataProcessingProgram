from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import ASCENDING
from datetime import datetime, timezone
import time
from multiprocessing import Process
import pandas as pd
import queue
import pandas as pd
from scipy.optimize import minimize
import numpy as np
from math import sqrt

def compile_information(change, scan_number, client, df1, df2):
    # Assuming 'TagID', 'ReceiverID', and 'radius' are fields in the change document
    # Safely get 'fullDocument' from 'change'; default to an empty dict if not found
    full_document = change.get('fullDocument', {})

    # Now, safely attempt to access each field in 'fullDocument'
    tag_id = full_document.get('TagID')
    receiver_id = full_document.get('ReceiverID')
    radius = full_document.get('radius')
        
    # Get receiver location information, x_location and y_location from the 'Receiver' collection
    db = client['Demo1']
    collection = db['Receivers']

    # Get the receiver location information for the given receiver ID
    receiver_location_info = collection.find_one({'ReceiverID': receiver_id})
    
    # Extract the x_location and y_location from the receiver location information
    x_location = receiver_location_info['x_location']
    y_location = receiver_location_info['y_location']
    # define which dataframe to add to
    if scan_number % 2 == 1:  # If the scan number is odd, then add to df1
        df1.loc[len(df1.index)] = [scan_number, tag_id, receiver_id, radius, x_location, y_location]
        # Add the extracted fields to the dataframe
            
    else:  # If the scan number is even
        df2.loc[len(df2.index)] = [scan_number, tag_id, receiver_id, radius, x_location, y_location]
            
def trialateration(scan_number, df1, df2):
    def trilateration_calc(scan_df):
        # The objective function calculates the sum of squared differences
        def objective(point, locations, radii):
            distances = np.sqrt((locations[:, 0] - point[0])**2 + (locations[:, 1] - point[1])**2)
            # Weights inversely proportional to the radii, smaller radii get larger weights
            weights = 1 / radii
            weighted_errors = weights * (distances - radii)**2
            return np.sum(weighted_errors)

        locations = scan_df[['x_location', 'y_location']].values
        radii = scan_df['radius'].values
        initial_guess = [np.mean(locations[:, 0]), np.mean(locations[:, 1])]
        result = minimize(objective, initial_guess, args=(locations, radii))

        TagID = scan_df['TagID'].iloc[0]

        if result.success:
            optimized_location = result.x
            x_location = float(round(optimized_location[0], 1))
            y_location = float(round(optimized_location[1], 1))

            # Apply constraints to x and y locations
            if x_location < 0:
                x_location = 1.0
            elif x_location > 25:
                x_location = 24.0

            if y_location > 13:
                y_location = 12.0

            return TagID, x_location, y_location
        else:
            print(f"Optimization failed for TagID {TagID}.")
            return TagID, None, None
    def upload_to_db(TagID, x_location, y_location):
        # Connect to MongoDB database and upload the result
        uri = "mongodb+srv://<username>:<password>@cluster0.wodzmcz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri, server_api=ServerApi('1'))

        db = client['Demo1'] 
        collection = db['T_tags']
            # Update the document in the collection
        # Define the fields to update
        update_fields = {
            'Time_of_update': datetime.now(timezone.utc),
            'x_location': x_location,
            'y_location': y_location,
        }
        # find current location for the given TagID
            # Get the receiver location information for the given receiver ID
        receiver_location_info = collection.find_one({'TagID': TagID})
        
        # Extract the x_location and y_location from the receiver location information
        old_x_location = receiver_location_info['x_location']
        old_y_location = receiver_location_info['y_location']
        # Calculate the Euclidean distance
        distance = sqrt((x_location - old_x_location)**2 + (y_location - old_y_location)**2)

        if distance >= 20:
            # If the distance is >= 25, do not update
            return "No update due to significant location change."
        elif distance >= 10:
            # If the distance is between 15 and 25, take an average
            x_location = (old_x_location + x_location) / 2
            y_location = (old_y_location + y_location) / 2
        if x_location<=0:
            x_location = 1.0
        if x_location>=24:
            x_location = 24.0

        if y_location<=0:
            y_location = 1.0
        if y_location>=13:
            y_location = 12.0

        try:
            TagIDint = int(TagID)
            update_result = collection.update_one(
                {'TagID': TagIDint, 'Active': True},  # Ensure we are updating an active record
                {'$set': update_fields}
            )

            if update_result.matched_count:
                print(f"Location updated for TagID {TagID}, x location = {x_location}, y location = {y_location}.")
            else:
                print(f"No active document with TagID {TagID} found to update.")
        except Exception as e:
            print(f"An error occurred while updating the document: {e}")
    def calculate_two_scanners(scan_df):
        locations = scan_df[['x_location', 'y_location']].values
        radii = scan_df['radius'].values

        total_radii = sum(radii)

        # Calculate the weighted position based on the radii
        weighted_x = (locations[0][0] * radii[1] + locations[1][0] * radii[0]) / total_radii
        weighted_y = (locations[0][1] * radii[1] + locations[1][1] * radii[0]) / total_radii
        
        # Round the calculated positions to one decimal place
        x_location_rounded = float(round(weighted_x, 1))
        y_location_rounded = float(round(weighted_y, 1))

        TagID = scan_df['TagID'].iloc[0]
        return TagID, x_location_rounded, y_location_rounded

    # which dataframe to use - this doesn't copy the dataframe, it creates a variable that references the correct dataframe.
    if scan_number % 2 == 1:  # If the scan number is odd, then add to df2
        df =  df2
    else:  # If the scan number is even, then add to df1
        df =  df1     
    # Main processing block
    max_scan_number = df['scan_number'].max()
    max_scan_df = df[df['scan_number'] == max_scan_number]
    # print(df.head(20))
    number_of_tags = 1
    # Perform trilateration for each TagID in the maximum scan dataframe
    for tag in max_scan_df['TagID'].unique(): # Loops through all of the TagIDs in the current scan
        tag_df = max_scan_df[max_scan_df['TagID'] == tag] # Makes a dataframe for just the current TagID and just the current scan
            # Check if the tag has at least three entries
        if len(tag_df) == 0:
            print(f"No tags detected {tag}.")
            return
        if len(tag_df) == 1:
            x_location = tag_df['x_location'].iloc[0]
            y_location = tag_df['y_location'].iloc[0]
            TagID = tag_df['TagID'].iloc[0]
            if number_of_tags == 1:
                upload_to_db(TagID, x_location+0.5, y_location)
            if number_of_tags == 2:
                upload_to_db(TagID, x_location-0.5, y_location)
            if number_of_tags == 3:
                upload_to_db(TagID, x_location+0.5, y_location)
            if number_of_tags == 4:
                upload_to_db(TagID, x_location+0.5, y_location)
                print(f"TagID {tag} updated using one scanner.")
            number_of_tags += 1
            print(f"TagID {tag} updated using one scanner.")
        if len(tag_df) == 2:
            TagID, x_location, y_location = calculate_two_scanners(tag_df)
            upload_to_db(TagID, x_location, y_location)
            print(f"TagID {tag} updated using two scanners.")

        if len(tag_df) == 3:
            TagID, x_location, y_location = trilateration_calc(tag_df)
            upload_to_db(TagID, x_location, y_location)
            print(f"TagID {tag} updated using three scanners.")

def main():
    # Create an empty pandas dataframe
    df1 = pd.DataFrame(columns=['scan_number','TagID', 'ReceiverID', 'radius', 'x_location', 'y_location'])
    df2 = pd.DataFrame(columns=['scan_number','TagID', 'ReceiverID', 'radius', 'x_location', 'y_location'])

    uri = "mongodb+srv://<username>:<password>@cluster0.wodzmcz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    # Connect to the database
    db = client['Demo1']
    T_tags_local = db['T_tags_local']

    # Initialize the scan number
    old_scan_number = 1

    # Watch the collection
    try:
        # Define a change stream pipeline to filter for insert operations only
        pipeline = [
            {'$match': {'operationType': 'insert'}}
        ]
        # Use the pipeline in the watch method
        with T_tags_local.watch(pipeline, full_document='updateLookup') as stream:
            print("Watching for insertions. Press Ctrl+C to stop.")
            for change in stream:
                # At this point, every change is an insert, and fullDocument should always be present
                # Assuming 'Scan Number' and 'AreaCode' are fields in the change document
                scan_number = change.get('fullDocument', {}).get('Scan Number')  # Safely access nested fields
                if (scan_number == old_scan_number):
                    compile_information(change, scan_number, client, df1, df2)
                    # Got fields and added them to pandas dataframe [scan_number, tag_id, receiver_id, radius, x_location, y_location]
                else:
                    # still need to compile this entry to the next dataframe - compile information should handle this
                    compile_information(change, scan_number, client, df1, df2)
                    p = Process(target=trialateration, args=(scan_number, df1, df2))
                    p.start()
                    p.join()
                    old_scan_number = scan_number
                    print(f"Updated scan number = {scan_number}")
                
    except KeyboardInterrupt:
        # Handle any cleanup or pressing Ctrl+C to stop
        print("\nStopped watching for changes.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Perform any necessary cleanup here
        stream.close()
        client.close()
        print("Cleaned up resources.")

if __name__ == "__main__":
    main()
