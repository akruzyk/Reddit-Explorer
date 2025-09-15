import React from "react";
import { formatNumber } from "../utils/formatters";

const CategoryCard = ({ category, onClick, isActive }) => {
  return (
    <div
      className={`category-card p-5 rounded-xl cursor-pointer transition-all duration-300 ${
        isActive
          ? "bg-gradient-to-br from-indigo-500 to-purple-600 text-white border-2 border-indigo-500"
          : "bg-gradient-to-br from-gray-50 to-gray-100 hover:from-gray-100 hover:to-gray-200 border-2 border-transparent hover:border-indigo-500"
      } hover:transform hover:-translate-y-1 hover:shadow-lg`}
      onClick={onClick}
      role="button"
      tabIndex={0}
      aria-label={`Select ${category.name} category`}
      onKeyDown={(e) => e.key === "Enter" && onClick()}
    >
      <span className="text-2xl mb-3 block" aria-hidden="true">
        {category.icon}
      </span>
      <div className="text-lg font-semibold mb-1">{category.name}</div>
      <div className={`text-sm mb-2 ${isActive ? "opacity-90" : "opacity-70"}`}>
        {formatNumber(category.count)} communities
      </div>
      <div className={`text-xs leading-tight ${isActive ? "opacity-80" : "opacity-60"}`}>
        {category.description}
      </div>
    </div>
  );
};

export default React.memo(CategoryCard);
