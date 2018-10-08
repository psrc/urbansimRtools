# Capacity computation

The script `parcels_capacity.R` derives parcel-level capacity. The script `aggregate_capacity.R` aggregates the output of `parcels_capacity.R` to a higher level geography. 

## Parcel-level

The capacity is derived using a set of development proposals generated via an urbansim run. In that run, no age restriction on redevelopment was used. Thus, the set of proposals constitutes the full set of development options on each parcel during any urbansim run, conditioned on the given zoning. Proposals that were eliminated in the urbansim run due to being significantly smaller than the largest proposal on the respective parcel are not considered here.

Proposals are composed by components of (possibly) different type. In the script, the proposal set is split into three groups:

1. **Residential**: proposals placed on parcels that only contain residential proposals 
2. **Non-residential**: proposals placed on parcels that only contain non-residential proposals
3. **Mix-use**: proposals placed on parcels that contain both, residential and non-residential proposals or proposal components

From the first two groups, we only keep the maximum proposal per parcel, and only those that are larger than what is already built on the parcel. Both conditions are measured on proposed residential units for 1., and non-residential sqft for 2. 

The third group is decomposed into a residential and a non-residential sets of components and it is proceeded as with groups 1. and 2. in terms of reducing to one component per parcel at most. We do not eliminate components based on size if they belong to a proposal that has both, a residential and a non-residential component. Then the two sets of components are merged. For parcels for which the selected maximum residential and non-residential components belong to different proposals, one of the two proposal is randomly sampled. The user can set the propbability of sampling residential proposals in the object `res.ratio` (default is 50, i.e. equal chance).

Finally, the three groups are merged together. For non-residential components job capacity is derived from non-residential sqft, the corresponding zone and the building type, using the `building_sqft_per_job` table. 

The base year capacity is taken from a dataset of buildings exported in the base year into a csv file and aggregated to a parcel level dataset.  

The final parcel-level capacity quantities are derived either from the final merged proposal dataset (if there is any proposal for that parcel), or from the base year capacity (if no proposal for that parcel exists or made it through). 

The resulting dataset is written into an csv file.

## Aggregation

The output file from `parcels_capacity.R` can be aggregated into a higher level geography using the script `aggregate_capacity.R`. The level of aggregation is defined by the object `geography`. Set it to `NULL` if a regional aggregation is desired. The geography id (column `{geography}_id`) must be present in the parcel-level file `parcels_geos.csv ` placed in the `lookup.path`. If further columns are desired, e.g. geography name, set the name of the file that contains those columns to `geo.file.name` and identify these additional columns in `geo.file.attributes`.

The script can also compute columns of differences between the end year capacity and the base year, if `compute.difference` is set to `TRUE`. If desired, the final aggregated dataset is saves to a csv file. 



   

 