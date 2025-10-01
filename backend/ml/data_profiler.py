"""
Data Profiler - Advanced data analysis using ML techniques

Analyzes data characteristics, patterns, and quality issues.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime


class DataProfiler:
    """
    Advanced data profiling with ML-enhanced analysis
    """
    
    def profile_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Create comprehensive data profile
        
        Args:
            df: DataFrame to profile
            
        Returns:
            Detailed profile with statistics and patterns
        """
        profile = {
            "basic_stats": self._get_basic_stats(df),
            "column_profiles": self._profile_columns(df),
            "data_quality": self._assess_data_quality(df),
            "patterns_detected": self._detect_patterns(df),
            "correlations": self._find_correlations(df)
        }
        
        return profile
    
    def _get_basic_stats(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic statistics about the dataset"""
        return {
            "row_count": len(df),
            "column_count": len(df.columns),
            "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            "null_cells": df.isnull().sum().sum(),
            "null_percentage": (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            "duplicate_rows": df.duplicated().sum()
        }
    
    def _profile_columns(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Profile each column individually"""
        profiles = {}
        
        for col in df.columns:
            col_profile = {
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_percentage": float((df[col].isnull().sum() / len(df)) * 100),
                "unique_count": int(df[col].nunique()),
                "unique_percentage": float((df[col].nunique() / len(df)) * 100)
            }
            
            # Numeric columns
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update({
                    "min": float(df[col].min()) if not df[col].empty else None,
                    "max": float(df[col].max()) if not df[col].empty else None,
                    "mean": float(df[col].mean()) if not df[col].empty else None,
                    "median": float(df[col].median()) if not df[col].empty else None,
                    "std": float(df[col].std()) if not df[col].empty else None,
                    "zeros_count": int((df[col] == 0).sum()),
                    "negative_count": int((df[col] < 0).sum()) if df[col].dtype in ['int64', 'float64'] else 0
                })
            
            # Datetime columns
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                col_profile.update({
                    "min_date": str(df[col].min()),
                    "max_date": str(df[col].max()),
                    "date_range_days": (df[col].max() - df[col].min()).days if not df[col].empty else 0
                })
            
            # Text columns
            else:
                col_profile.update({
                    "max_length": int(df[col].astype(str).str.len().max()) if not df[col].empty else 0,
                    "min_length": int(df[col].astype(str).str.len().min()) if not df[col].empty else 0,
                    "avg_length": float(df[col].astype(str).str.len().mean()) if not df[col].empty else 0
                })
            
            profiles[col] = col_profile
        
        return profiles
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess overall data quality"""
        quality_score = 100.0
        issues = []
        
        # Check for missing values
        null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        if null_percentage > 10:
            quality_score -= 20
            issues.append(f"High null percentage: {null_percentage:.1f}%")
        elif null_percentage > 5:
            quality_score -= 10
            issues.append(f"Moderate null percentage: {null_percentage:.1f}%")
        
        # Check for duplicates
        dup_percentage = (df.duplicated().sum() / len(df)) * 100
        if dup_percentage > 5:
            quality_score -= 15
            issues.append(f"High duplicate rows: {dup_percentage:.1f}%")
        
        # Check for constant columns
        constant_cols = [col for col in df.columns if df[col].nunique() == 1]
        if constant_cols:
            quality_score -= 10
            issues.append(f"Constant columns found: {', '.join(constant_cols)}")
        
        # Check for high cardinality in text columns
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) > 0.95:
                issues.append(f"Very high cardinality in '{col}'")
        
        return {
            "quality_score": max(0, quality_score),
            "issues": issues,
            "recommendations": self._generate_quality_recommendations(issues)
        }
    
    def _generate_quality_recommendations(self, issues: List[str]) -> List[str]:
        """Generate recommendations based on quality issues"""
        recommendations = []
        
        for issue in issues:
            if "null" in issue.lower():
                recommendations.append("Consider imputation or filtering null values")
            elif "duplicate" in issue.lower():
                recommendations.append("Remove duplicate rows before loading")
            elif "constant" in issue.lower():
                recommendations.append("Drop constant columns as they provide no information")
            elif "cardinality" in issue.lower():
                recommendations.append("High cardinality column - consider hashing or categorization")
        
        return recommendations
    
    def _detect_patterns(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Detect common data patterns"""
        patterns = {
            "time_series": [],
            "categorical": [],
            "identifiers": [],
            "metrics": []
        }
        
        for col in df.columns:
            # Time series detection
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                patterns["time_series"].append(col)
            elif any(keyword in col.lower() for keyword in ['date', 'time', 'timestamp', 'created', 'updated']):
                patterns["time_series"].append(col)
            
            # Categorical detection
            if df[col].dtype == 'object' and df[col].nunique() / len(df) < 0.05:
                patterns["categorical"].append(col)
            
            # Identifier detection
            if any(keyword in col.lower() for keyword in ['id', 'key', 'code', 'uuid']):
                patterns["identifiers"].append(col)
            elif df[col].nunique() == len(df):  # Unique values
                patterns["identifiers"].append(col)
            
            # Metrics detection
            if pd.api.types.is_numeric_dtype(df[col]) and col not in patterns["identifiers"]:
                patterns["metrics"].append(col)
        
        return patterns
    
    def _find_correlations(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Find correlations between numeric columns"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numeric_cols) < 2:
            return []
        
        corr_matrix = df[numeric_cols].corr()
        correlations = []
        
        # Find strong correlations (> 0.7 or < -0.7)
        for i in range(len(numeric_cols)):
            for j in range(i + 1, len(numeric_cols)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:
                    correlations.append({
                        "column1": numeric_cols[i],
                        "column2": numeric_cols[j],
                        "correlation": float(corr_value),
                        "strength": "strong" if abs(corr_value) > 0.9 else "moderate"
                    })
        
        return correlations
    
    def suggest_transformations(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        """Suggest data transformations based on profiling"""
        suggestions = []
        
        # Suggest normalization for numeric columns with wide ranges
        for col in df.select_dtypes(include=[np.number]).columns:
            if df[col].std() > df[col].mean() * 2:
                suggestions.append({
                    "column": col,
                    "transformation": "normalize",
                    "reason": "High standard deviation suggests normalization needed"
                })
        
        # Suggest encoding for categorical columns
        for col in df.select_dtypes(include=['object']).columns:
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.1:
                suggestions.append({
                    "column": col,
                    "transformation": "one_hot_encode",
                    "reason": f"Low cardinality ({df[col].nunique()} values) - good for encoding"
                })
        
        # Suggest datetime parsing
        for col in df.columns:
            if df[col].dtype == 'object':
                sample = df[col].dropna().head(10)
                if any(pd.to_datetime(sample, errors='coerce').notna()):
                    suggestions.append({
                        "column": col,
                        "transformation": "parse_datetime",
                        "reason": "Column contains datetime strings"
                    })
        
        return suggestions
