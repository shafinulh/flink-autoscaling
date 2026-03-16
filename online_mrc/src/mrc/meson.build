subdir('average_eviction_time')
subdir('evicting_map')
subdir('evicting_quickmrc')
# subdir('goel_quickmrc')  # disabled: requires private quickmrc submodule
goel_quickmrc_dep = declare_dependency()  # stub so dependents still compile
subdir('mimir')
subdir('parda_shards')
subdir('quickmrc')
subdir('olken')

# SHARDS depends on Olken, therefore, it must be below!
subdir('shards')