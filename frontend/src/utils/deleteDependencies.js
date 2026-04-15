export const flattenDependencyItems = (dependencies = []) =>
  (dependencies || []).flatMap((dependency) =>
    (dependency.linked_items || []).map((item) => ({
      ...item,
      dependency_type: dependency.type,
      dependency_module: dependency.module,
    }))
  );

export const summarizeDependencies = (dependencies = []) =>
  (dependencies || [])
    .map((dependency) => `${dependency.count} ${dependency.type} record${dependency.count === 1 ? '' : 's'}`)
    .join(', ');
