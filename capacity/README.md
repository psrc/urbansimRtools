# Capacity computation

The script `parcels_capacity.R` derives parcel-level capacity. The script `aggregate_capacity.R` aggregates the output from the former script to a higher level geography. 

## Parcel-level

The capacity is derived using a set of development proposals generated via an urbansim run. In that run, no age restriction on redevelopment was used. Thus, the set of proposals constitutes the full set of development options on each parcel during any urbansim run, conditioned on the given zoning.

Proposals are composed by components of (possibly) different type. In the script, the proposal set is split into three groups:

1. Residential proposals only
2. Non-residential proposals only
3. Mix-use proposals

From the first two groups, we only keep the maximum proposal per parcel, and only those that are larger than what is already built on the parcel. Both conditions are measured on proposed residential units for 1., and non-residential sqft for 2. 

The third group is decomposed into a residential and a non-residential subset and proceed as with groups 1. and 2. in terms of reducing to one proposal per parcel at most. We do not eliminate proposals based on size if they contain both, a residential and a non-residential component. Then the two subsets are merged. For parcels for which the selected maximum residential and non-residential components belong to different proposals, one of the two proposal is randomly sampled. The user can set a propbability of sampling residential proposals in the object `res.ratio` (default is equal probability, 50).

Finally, the three groups are merged together.


 