# Capacity computation

The script `parcels_capacity.R` derives parcel-level capacity. Multiple files generated via `parcels_capacity.R` can be merged together using `merge_files_for_different_ratios.R`. The script `aggregate_capacity.R` aggregates the output of either of the two scripts to a higher level geography. 

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

## Merging 

One can run the `parcels_capacity.R` script multiple times, each time with a different residential sampling ratio (`res.ratio`). The script `merge_files_for_different_ratios.R` merges the results into one file. It adds the ratio value to the corresponding column names.

## Aggregation

The output files from either `parcels_capacity.R` or `merge_files_for_different_ratios.R` can be aggregated into a higher level geography using the script `aggregate_capacity.R`. The level of aggregation is defined by the object `geography`. Set it to `NULL` if a regional aggregation is desired. The geography id (column `{geography}_id`) must be present in the parcel-level file `parcels_geos.csv ` placed in the `lookup.path`. If further columns are desired, e.g. geography name, set the name of the file that contains those columns to `geo.file.name` and identify these additional columns in `geo.file.attributes`.

The script can also compute columns of differences between the end year capacity and the base year, if `compute.difference` is set to `TRUE`. If desired, the final aggregated dataset is saves to a csv file. 

## Advanced Aggregation

The script `aggregate_capacity_advanced.R` also allows to aggregate parcel-level capacity to a higher geography. In addition to the simple aggregation as described above, it allows to aggregate to sub-levels within a geography, e.g. city and TODs within city. One can also split the aggregation by groups of residential density. 

Here the `geography` object can be a vector of the geographic groups and sub-groups. The density grouping is defined in the object `density.splits`. It is a list of density definitions, which is either `NA` (no grouping), or a single number defining the exact density, or a vector of size two defining the lower and upper bounds (treated as right-open intervals). These groups do not have to be mutually exclusive.

The script generates one file per density group. The object `density.names` is a character vector that distinguishes the file names. The order of its elements corresponds to the elements in `density.splits`. I.e., the file corresponding to density `density.splits[[i]]` will contain  `density.names[i]` in its name.

The output files are stored in the directory defined by the object `output.dir`, by default `capres-{current_date}`.

As in the case of simple aggregation, all geography ids (constructed from `{geography}_id`) must be present in the parcel-level file defined in `parcel.lookup.name` placed in the `lookup.path`.

If further columns are desired, e.g. geography names, set the name of the files that contain those columns to `geo.file.name`. It is a list with elements corresponding to each element in the `geography` object and values being the file names. Those files have to contain the corresponding `{geography}_id` column. Object `geo.file.attributes` has the same structure as `geo.file.name` and defines the additional columns from each file to be included in the resulting file.

As above, the object `compute.difference` determines if columns of differences between the end year capacity and the base year should be included.


   

 