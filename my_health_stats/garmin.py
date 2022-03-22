from garminconnect import Garmin
from loguru import logger

class MyGarmin:

    def __init__(self, username, password):
        self.api = Garmin(username, password)
        self.api.login()

    def get_data(self):
        ## Get activity data for today 'YYYY-MM-DD'
        logger.info(self.api.get_stats('2021-10-24'))
        
        ## Get activity data (to be compatible with garminconnect-ha)
        logger.info(self.api.get_user_summary('2021-10-24'))
        
        ## Get body composition data for today 'YYYY-MM-DD' (to be compatible with garminconnect-ha)
        logger.info(self.api.get_body_composition('2021-10-24'))
        
        ## Get body composition data for multiple days 'YYYY-MM-DD' (to be compatible with garminconnect-ha)
        logger.info(self.api.get_body_composition('2021-10-24', '2021-10-24'))
        
        ## Get stats and body composition data for today 'YYYY-MM-DD'
        logger.info(self.api.get_stats_and_body('2021-10-24'))
        
        # USER STATISTICS LOGGED
        
        ## Get steps data for today 'YYYY-MM-DD'
        logger.info(self.api.get_steps_data('2021-10-24'))
        
        ## Get heart rate data for today 'YYYY-MM-DD'
        logger.info(self.api.get_heart_rates('2021-10-24'))
        
        ## Get resting heart rate data for today 'YYYY-MM-DD'
        logger.info(self.api.get_rhr_day('2021-10-24'))
        
        ## Get hydration data 'YYYY-MM-DD'
        logger.info(self.api.get_hydration_data('2021-10-24'))
        
        ## Get sleep data for today 'YYYY-MM-DD'
        logger.info(self.api.get_sleep_data('2021-10-24'))
        
        ## Get stress data for today 'YYYY-MM-DD'
        logger.info(self.api.get_stress_data('2021-10-24'))
        
        ## Get respiration data for today 'YYYY-MM-DD'
        logger.info(self.api.get_respiration_data('2021-10-24'))
        
        ## Get SpO2 data for today 'YYYY-MM-DD'
        logger.info(self.api.get_spo2_data('2021-10-24'))
        
        ## Get max metric data (like vo2MaxValue and fitnessAge) for today 'YYYY-MM-DD'
        logger.info(self.api.get_max_metrics('2021-10-24'))
        
        ## Get personal record for user
        logger.info(self.api.get_personal_record())
        
        ## Get earned badges for user
        logger.info(self.api.get_earned_badges())
        
        ## Get adhoc challenges data from start and limit
        logger.info(self.api.get_adhoc_challenges(1, 100))  # 1=start, 100=limit
        
        # Get badge challenges data from start and limit
        logger.info(self.api.get_badge_challenges(1, 100))  # 1=start, 100=limit
        
        # ACTIVITIES
        
        # Get activities data from start and limit
        activities = self.api.get_activities(0, 1)  # 0=start, 1=limit
        logger.info(activities)
        
        # Get activities data from startdate 'YYYY-MM-DD' to enddate 'YYYY-MM-DD', with (optional) activitytype
        # Possible values are [cycling, running, swimming, multi_sport, fitness_equipment, hiking, walking, other]
        activities = self.api.get_activities_by_date('2021-08-01', '2021-08-10')
        
        # Get last activity
        logger.info(self.api.get_last_activity())
        
        ## Download an Activity
        for activity in activities:
            activity_id = activity["activityId"]
            logger.info("api.download_activities(%s)", activity_id)
        
            gpx_data = self.api.download_activity(activity_id, dl_fmt=api.ActivityDownloadFormat.GPX)
            output_file = f"./{str(activity_id)}.gpx"
            with open(output_file, "wb") as fb:
                fb.write(gpx_data)
        
            tcx_data = self.api.download_activity(activity_id, dl_fmt=api.ActivityDownloadFormat.TCX)
            output_file = f"./{str(activity_id)}.tcx"
            with open(output_file, "wb") as fb:
                fb.write(tcx_data)
        
            zip_data = self.api.download_activity(activity_id, dl_fmt=api.ActivityDownloadFormat.ORIGINAL)
            output_file = f"./{str(activity_id)}.zip"
            with open(output_file, "wb") as fb:
                fb.write(zip_data)
        
            csv_data = self.api.download_activity(activity_id, dl_fmt=api.ActivityDownloadFormat.CSV)
            output_file = f"./{str(activity_id)}.csv"
            with open(output_file, "wb") as fb:
                fb.write(csv_data)
        
        ## Get activity splits
        first_activity_id = activities[0].get("activityId")
        owner_display_name = activities[0].get("ownerDisplayName")
        
        logger.info(self.api.get_activity_splits(first_activity_id))
        
        ## Get activity split summaries for activity id
        logger.info(self.api.get_activity_split_summaries(first_activity_id))
        
        ## Get activity weather data for activity
        logger.info(self.api.get_activity_weather(first_activity_id))
        
        ## Get activity hr timezones id
        logger.info(self.api.get_activity_hr_in_timezones(first_activity_id))
        
        ## Get activity details for activity id
        logger.info(self.api.get_activity_details(first_activity_id))
        
        # ## Get gear data for activity id
        logger.info(self.api.get_activity_gear(first_activity_id))
        
        ## Activity self evaluation data for activity id
        logger.info(self.api.get_activity_evaluation(first_activity_id))
    
